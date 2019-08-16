/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate patmat_core as pattern_matching;
extern crate abomonation;
extern crate bodyparser;
extern crate byteorder;
extern crate env_logger;
extern crate iron;
extern crate persistent;
extern crate rand;
extern crate rust_graph;
extern crate serde_cbor;
extern crate serde_json;
extern crate timely;

use abomonation::Abomonation;

use clap::{App, Arg};

use std::cell::RefCell;
use std::fs;
use std::marker::{Send, Sync};
use std::panic;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use pattern_matching::property::{parse_property_tree, parse_result_blueprint, PropertyCache};
use rust_graph::graph_impl::*;
use rust_graph::io::serde::*;
use rust_graph::prelude::*;
use rust_graph::property::*;

use timely::communication::Allocate;
use timely::dataflow::operators::*;

use iron::prelude::*;
use iron::status::Status;
use iron::typemap::Key;
use pattern_matching::conf::{GraphConf, JsonConf};
use pattern_matching::join::generic_join::{to_original_ids, ResultBuilder};
use pattern_matching::join::*;
use pattern_matching::storage::fs::*;
use pattern_matching::utils::FixedString;
use pattern_matching::{DefaultProperty, Driver, Id, IdType, SimpleLabel, NAME, VERSION};
use persistent::Read;

use serde_json::json;
use serde_json::Value as JsonValue;

type TimeStamp = u32;

#[derive(Copy, Clone)]
pub struct ConfState;

impl Key for ConfState {
    type Value = (
        Vec<String>,
        BasicStorageConf,
        Arc<TypedStaticGraph<u32, FixedString, FixedString, Directed>>,
        PropertyCacheHolder<u32, RocksProperty>,
        bool,
    );
}

pub struct PropertyCacheHolder<I: IdType, PG: PropertyGraph<I>> {
    caches: Vec<Rc<RefCell<PropertyCache<I, PG>>>>,
}

impl<I: IdType, PG: PropertyGraph<I>> PropertyCacheHolder<I, PG> {
    pub fn new(caches: Vec<Rc<RefCell<PropertyCache<I, PG>>>>) -> Self {
        PropertyCacheHolder { caches }
    }

    pub fn get(&self, id: usize) -> Rc<RefCell<PropertyCache<I, PG>>> {
        self.caches[id].clone()
    }

    pub fn len(&self) -> usize {
        self.caches.len()
    }
}

unsafe impl<I: IdType, PG: PropertyGraph<I>> Send for PropertyCacheHolder<I, PG> {}

unsafe impl<I: IdType, PG: PropertyGraph<I>> Sync for PropertyCacheHolder<I, PG> {}

fn define_driver<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    driver: Rc<
        GenericJoinDriver<
            u32,
            FixedString,
            FixedString,
            TypedStaticGraph<u32, FixedString, FixedString, Directed>,
            RocksProperty,
            GenericJoinOrder,
            u32
        >,
    >,
    mut probe: timely::dataflow::ProbeHandle<u32>,
    counter: Rc<RefCell<usize>>,
) {
    let _driver = driver.clone();
    let is_count_only = _driver.get_conf().is_count_only;
    let _result_builder = _driver.get_result_builder();
    let result_len = match &_result_builder {
        Some(result_builder) => result_builder.get_result_length(),
        _ => 0,
    };
    let no_result_builder = _result_builder.is_none();

    worker.dataflow(move |scope| {
        if is_count_only || no_result_builder {
            let count_stream = _driver.build_dataflow(scope).map(|x| x[0] as usize);
            count_stream
                .exchange(|_| 0u64)
                .inspect_batch(move |_, x| {
                    *(counter.borrow_mut()) += x.iter().sum::<usize>();
                })
                .probe_with(&mut probe);
        } else {
            let result_builder = _driver.clone().get_result_builder().unwrap();
            let original_stream = _driver.build_dataflow(scope);
            let client = Rc::new(reqwest::Client::new());

            info!("Start extracting result from user's query");

            let mut u8_stream = result_builder.to_u8stream(original_stream);

            for i in 0usize..result_len {
                let _result_builder = _driver.get_result_builder().clone().unwrap();

                u8_stream = u8_stream
                    .exchange(move |u8_result| {
                        (to_original_ids::<u32>(&u8_result, result_len)[i]) as u64
                    })
                    .map_in_place(move |mut u8_result| {
                        _result_builder
                            .append_value(&mut u8_result, i)
                            .expect("Failed to append result");
                    });
            }

            result_builder
                .read_result_values(u8_stream, client)
                .map(|_| 1usize)
                .exchange(|_| 0u64)
                .inspect_batch(move |_, x| {
                    *(counter.borrow_mut()) += x.iter().sum::<usize>();
                })
                .probe_with(&mut probe);
        }
    });
    if let Err(error) = driver.load_locally(worker) {
        send_to_server(&_result_builder, &json!({"error": format!("{:?}", error)}));
    } else {
        let result_perfstats = driver.get_perf_stats();
        if let Err(error) = result_perfstats {
            send_to_server(&_result_builder, &json!({"error": format!("{:?}", error)}));
        } else {
            let perfstats = result_perfstats.unwrap();
            info!("Worker {}, {:?}", worker.index(), perfstats);
            send_to_server(&_result_builder, &json!({"perfstats": perfstats, "worker": worker.index()}));
        }
    }
}

fn send_to_server<I: IdType + Abomonation, PG: PropertyGraph<I> + 'static>(_result_builder: &Option<Rc<ResultBuilder<I, PG>>>, value: &JsonValue) {
    if let Some(result_builder) = _result_builder {
        let client = reqwest::Client::new();
        if let Some(uri) = result_builder.get_server_uri() {
            let _res = client
                .post(uri)
                .json(value)
                .send();
        }
    }
}

fn run_driver(
    graph_fs: GraphConf,
    server_uri: Option<String>,
    timely_args: Vec<String>,
    lru_size: usize,
    port: usize,
) {
    let conf_fs = graph_fs.storage.clone();
    let prefix = graph_fs.prefix.unwrap();

    let fs = BasicStorage::with_conf(conf_fs.clone()).unwrap();
    let path = fs.get_data(&prefix).unwrap();
    let node_path = fs
        .get_data(format!("{}.nodes.prop", prefix).as_str())
        .unwrap();
    let edge_path = fs
        .get_data(format!("{}.edges.prop", prefix).as_str())
        .unwrap();
    info!("Loading {:?}", path);
    let _graph = Arc::new(unwrap_or_panic!(Deserializer::import::<
        TypedStaticGraph<u32, FixedString, FixedString, Directed>,
        _,
    >(path)));

    let property = RocksProperty::open(node_path, edge_path, _graph.is_directed(), true);

    let _property_graph = match property {
        Ok(property_graph) => Some(Arc::new(property_graph)),
        _ => None,
    };

    let mut caches = Vec::new();
    for _ in 0..timely_args[1]
        .clone()
        .parse::<usize>()
        .expect("Worker number cannot be parsed to usize")
        {
            let cache = Rc::new(RefCell::new(PropertyCache::<u32>::new(
                _property_graph.clone(),
                lru_size as usize,
                false,
                true,
            )));
            caches.push(cache);
        }

    let property_caches = PropertyCacheHolder::new(caches);

    info!("Start PatMat @ http://localhost: {}", port);
    let send_to_server = if server_uri.is_some() { true } else { false };

    if send_to_server {
        let client = reqwest::Client::new();
        let post = json!({"ServerStarts": true, "port": port});
        let res = client.post(&server_uri.unwrap()).json(&post).send();
        if let Err(send_error) = res {
            error!("Uri sending error: {:?}", send_error);
        }
    }

    let mut chain = Chain::new(index);
    chain.link(Read::<ConfState>::both((
        timely_args,
        conf_fs,
        _graph,
        property_caches,
        send_to_server,
    )));
    Iron::new(chain)
        .http(format!("localhost:{}", port))
        .unwrap();
}

fn index(req: &mut Request) -> IronResult<Response> {
    let req_json = req.get::<bodyparser::Json>();
    if req_json.is_err() {
        Ok(Response::with((
            Status::BadRequest,
            "{ \"Error\": \"Request does not contain a valid JSON.\" }",
        )))
    } else if let Some(req_body) = req_json.unwrap() {
        let data = req
            .get::<Read<ConfState>>()
            .expect("Failed to get global configuration at server endpoint.");
        let send_to_server: bool = data.4;

        if send_to_server
            && (req_body.get("cache_size").is_none()
            || req_body.get("plan").is_none()
            || req_body.get("server_uri").is_none())
            {
                Ok(Response::with((
                    Status::BadRequest,
                    "Usage: {\"cache_size\": <cache_size>, \"plan\": <plan_path>, \"server_uri\": <server_uri>}",
                )))
            } else if req_body.get("cache_size").is_none() || req_body.get("plan").is_none() {
            Ok(Response::with((
                Status::BadRequest,
                "Usage: {\"cache_size\": <cache_size>, \"plan\": <plan_path>}",
            )))
        } else {
            let cache_size = {
                let value = req_body.get("cache_size").unwrap();
                if value.as_str().is_some() && value.as_str().unwrap().parse::<usize>().is_ok() {
                    value.as_str().unwrap().parse::<usize>().unwrap()
                } else if value.as_u64().is_some() {
                    value.as_u64().unwrap() as usize
                } else {
                    return Ok(Response::with((
                        Status::BadRequest,
                        "Usage: {\"cache_size\": <cache_size>, \"plan\": <plan_path>, \"server_uri\": <server_uri>}",
                    )));
                }
            };

            let plan = req_body.get("plan").unwrap().as_str().unwrap().to_string();
            let server_uri = if send_to_server {
                Some(
                    req_body
                        .get("server_uri")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string(),
                )
            } else {
                None
            };
            let _server_uri = server_uri.clone();
            let plan_clone = plan.clone();
            let plan_path = Path::new(&plan_clone);

            if !(plan_path.exists()) {
                Ok(Response::with((
                    Status::BadRequest,
                    "{ \"Error\": \"Plan path does not exist.\" }",
                )))
            } else {
                let timely_args = data.0.clone();
                let work_number = timely_args[1]
                    .parse::<usize>()
                    .expect("Worker number cannot be parsed to usize");
                let _conf = unwrap_or_panic!(GenericJoinConf::from_json_file(&plan));
                let exp_cache = Arc::new(parse_property_tree(_conf.query.property_tree.clone()));
                let result_blueprint =
                    Arc::new(parse_result_blueprint(_conf.query.property_tree.clone()));
                let empty_cypher_tree = _conf.query.property_tree.is_empty();

                timely::execute_from_args(timely_args.into_iter(), move |worker| {
                    let index = worker.index();

                    let count = Rc::new(RefCell::new(0));
                    let _count = count.clone();

                    let conf = _conf.clone();
                    let graph = data.2.clone();
                    let property_cache = data.3.get(index % work_number);
                    {
                        let mut mut_property_cache = property_cache.borrow_mut();
                        mut_property_cache.resize(cache_size.clone());
                    }

                    let result_builder = Rc::new(ResultBuilder::new(
                        result_blueprint.clone(),
                        property_cache.clone(),
                        server_uri.clone(),
                    ));

                    let _inner_option =
                        GenericJoinDriver::<TimeStamp, _, _, _, _, GenericJoinOrder, u32>::with_conf(
                            conf,
                            graph,
                            property_cache,
                        );
                    

                    if let Err(error) = _inner_option {
                        if let Some(uri) = &_server_uri {
                            let client = reqwest::Client::new();
                            let post = json!({"error": format!("{:?}", error)});
                            let _res = client.post(uri).json(&post).send();
                        }
                    } else {
                        let mut _inner = _inner_option.unwrap();
                        if !empty_cypher_tree {
                            _inner = _inner.with_expression_cache(exp_cache.clone())
                                .with_result_builder(result_builder.clone());
                        }

                        let driver = Rc::new(_inner);
                        let probe = driver.probe().clone();

                        info!("Worker {} start loading locally.", index);
                        define_driver(worker, driver, probe, _count);

                        if index == 0 {
                            println!(
                                r#"
                            **************************************
                                    Total # results: {}
                            **************************************

                         "#,
                                *count.borrow()
                            );

                            if let Some(uri) = &_server_uri {
                                let client = reqwest::Client::new();
                                let post = json!({"EndOfAll": true, "count": *count.borrow()});
                                let _res = client.post(uri).json(&post).send();
                            }
                            let data = format!("{{ \"count\": \"{:?}\" }}", *count.borrow());
                            fs::write("patmat_result.txt", data).expect("Unable to write file");

                        }
                    }
                    



                })
                    .expect("Failed to start timely.");
                let result = fs::read_to_string("patmat_result.txt");

                if let Ok(response) = result {
                    Ok(Response::with((Status::Ok, response)))
                } else {
                    Ok(Response::with((Status::Ok, "None")))
                }
            }
        }
    } else {
        Ok(Response::with((
            Status::BadRequest,
            "{ \"Error\": \"Request does not contain a valid JSON.\" }",
        )))
    }
}

fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Info);
    builder.init();

    let matches = App::new(NAME)
        .version(VERSION)
        .about("Run pattern matching")
        .args(&[
            Arg::with_name("graph_conf")
                .required(true)
                .help("The graph configuration file")
                .takes_value(true)
                .index(1),
            Arg::with_name("lru_cache_size")
                .required(true)
                .help("Initial size of lru cache")
                .takes_value(true)
                .index(2),
            Arg::with_name("port")
                .required(true)
                .help("The port of patmat server")
                .takes_value(true)
                .index(3),
            Arg::with_name("server_uri")
                .short("s")
                .long("server_uri")
                .help("Server URI to send finishing sign of graph loading.")
                .takes_value(true),
            Arg::with_name("workers")
                .short("w")
                .long("workers")
                .required(true)
                .help("Number of workers for Timely")
                .takes_value(true),
            Arg::with_name("machines")
                .short("n")
                .long("machines")
                .help("Number of machines for Timely")
                .takes_value(true),
            Arg::with_name("processor")
                .short("p")
                .long("processor")
                .help("Identity of this processor, from 0 to n (excluded)")
                .takes_value(true),
            Arg::with_name("host_file")
                .short("h")
                .long("hostfile")
                .help(r#"
                    A text file whose lines are <hostname:port> in order of process identity.
                    If not specified, `localhost` will be used, with port numbers increasing from 2101
                    (chosen arbitrarily)."#
                )
                .takes_value(true),
        ]).get_matches();

    let graph_conf_path = matches.value_of("graph_conf").unwrap();
    let lru_size = matches
        .value_of("lru_cache_size")
        .unwrap()
        .to_string()
        .parse::<usize>()
        .expect("Lru cache size arg cannot be parsed to usize");
    let port = matches
        .value_of("port")
        .unwrap()
        .to_string()
        .parse::<usize>()
        .expect("Port arg cannot be parsed to usize");

    let server_uri = match matches.value_of("server_uri") {
        Some(uri) => Some(uri.to_string()),
        None => None,
    };

    let graph_conf =
        GraphConf::from_json_file(graph_conf_path).expect("Read graph configuration error");

    // Fill the timely arguments
    let mut timely_args = Vec::new();
    if matches.is_present("workers") {
        timely_args.push("-w".to_string());
        timely_args.push(matches.value_of("workers").unwrap().to_string());
    }

    if matches.is_present("machines") {
        timely_args.push("-n".to_string());
        timely_args.push(matches.value_of("machines").unwrap().to_string());
    }

    if matches.is_present("processor") {
        timely_args.push("-p".to_string());
        timely_args.push(matches.value_of("processor").unwrap().to_string());
    }

    if matches.is_present("host_file") {
        timely_args.push("-h".to_string());
        timely_args.push(matches.value_of("host_file").unwrap().to_string());
    }

    run_driver(graph_conf, server_uri, timely_args, lru_size, port);
}