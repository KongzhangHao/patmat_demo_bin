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
extern crate env_logger;
extern crate patmat_core as pattern_matching;
extern crate rust_graph;
extern crate timely;
extern crate walkdir;

use clap::{App, Arg};
use pattern_matching::io::TextGraphReader;
use pattern_matching::storage::fs::*;
use pattern_matching::storage::partition::Partitioner;
use pattern_matching::utils::FixedString;
use pattern_matching::{Id, LabelType, SimpleLabel, NAME, VERSION};
use rust_graph::prelude::Void;
use rust_graph::graph_impl::GraphImpl::*;
use rust_graph::io::{CSVReader, HDFSReader};
use std::path::PathBuf;
use walkdir::{DirEntry, WalkDir};

type TimeStamp = u32;

fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Info);
    builder.init();

    let matches = App::new(NAME)
        .version(VERSION)
        .about("Partition a graph.")
        .args(&[
            Arg::with_name("edge_file")
                .long_help("Path to the edge file")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("work_dir")
                .long_help("Path to the work directory")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("persist_data")
                .long_help("Name of persistent data directory")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("data_prefix")
                .long_help("The prefix of exported graph")
                .required(true)
                .takes_value(true)
                .index(4),
            Arg::with_name("node_file")
                .short("N")
                .long("node")
                .long_help("Path to the node file")
                .takes_value(true),
            Arg::with_name("separator")
                .short("s")
                .long("sep")
                .long_help("Allowed separator: [comma|space|tab]")
                .default_value("comma")
                .takes_value(true),
            Arg::with_name("has_headers")
                .short("H")
                .long("headers")
                .long_help("Use when the csv files have headers"),
            Arg::with_name("buffer")
                .short("b")
                .long("buffer")
                .long_help("The number of lines to read form csv files each time")
                .takes_value(true),
            Arg::with_name("is_flexible")
                .short("F")
                .long("flexible")
                .long_help(
                    "Use when the csv files may have different number of fields in each row",
                ),
            Arg::with_name("reorder_node_id")
                .short("r")
                .long("reorder")
                .long_help("Re-order node ids according to their degrees"),
//            Arg::with_name("full_rep")
//                .short("f")
//                .long("full")
//                .long_help("Whether graph file is copied across all workers or not"),
            Arg::with_name("sync")
                .short("c")
                .long("sync")
                .long_help("Whether to synchronize or not"),
            Arg::with_name("workers")
                .short("w")
                .help("Number of workers for Timely")
                .takes_value(true),
            Arg::with_name("machines")
                .short("n")
                .help("Number of machines for Timely")
                .takes_value(true),
            Arg::with_name("processor")
                .short("p")
                .help("The index of the processor")
                .takes_value(true),
            Arg::with_name("host_file")
                .short("h")
                .help("The path to the host file for Timely")
                .takes_value(true),
        ])
        .get_matches();

    let node_file = matches.value_of("node_file").map(|s| s.to_owned());
    let edge_file = matches.value_of("edge_file").unwrap().to_owned();

    let mut is_hdfs = false;

    let node_path = match node_file {
        Some(p) => {
            if p.to_lowercase().starts_with("hdfs://") {
                is_hdfs = true;
            }

            vec![p]
        }
        None => Vec::new(),
    };

    if edge_file.to_lowercase().starts_with("hdfs://") {
        is_hdfs = true;
    } else if is_hdfs {
        panic!("Edge path should start with 'hdfs://'");
    };

    let edge_path = vec![edge_file];

    let separator = matches.value_of("separator").unwrap().to_owned();

    let has_headers = matches.is_present("has_headers");
    let is_flexible = matches.is_present("is_flexible");
    let buffer = matches
        .value_of("buffer")
        .map(|x| x.parse::<usize>().unwrap());

    let data_prefix = matches.value_of("data_prefix").unwrap().to_owned();
    let workdir = matches.value_of("work_dir").unwrap().to_owned();
    let persist_data = matches.value_of("persist_data").unwrap().to_owned();

    let reorder_node_id = matches.is_present("reorder_node_id");
    let full_rep = false; //matches.is_present("full_rep");
    let sync = matches.is_present("sync");

    // Fill the timely arguments
    let mut timely_args = Vec::new();
    if matches.is_present("workers") {
        timely_args.push("-w".to_string());
        timely_args.push(matches.value_of("workers").unwrap().to_string());
    }

    let mut num_of_machines = 1;
    if matches.is_present("machines") {
        timely_args.push("-n".to_string());
        let machines = matches.value_of("machines").unwrap().to_string();
        num_of_machines = machines.parse().unwrap();

        timely_args.push(machines);
    }

    if matches.is_present("processor") {
        timely_args.push("-p".to_string());
        timely_args.push(matches.value_of("processor").unwrap().to_string());
    }

    if matches.is_present("host_file") {
        timely_args.push("-h".to_string());
        timely_args.push(matches.value_of("host_file").unwrap().to_string());
    }

    timely::execute_from_args(timely_args.into_iter(), move |mut worker| {
        let fs = BasicStorage::with_conf(BasicStorageConf {
            workdir: workdir.clone(),
            persist_data: persist_data.clone(),
            temp_data: None,
        })
        .expect("Initialise BasicStorage error");

        let index = worker.index();

        let text_graph_reader = if index == 0 {

            if is_hdfs {
                let rdr = Box::new(
                    HDFSReader::new(node_path.clone(), edge_path.clone())
                        .with_separator(&separator)
                        .headers(has_headers)
                        .flexible(is_flexible),
                );
                let static_ref = Box::leak(rdr);

                TextGraphReader::<u32, FixedString, FixedString>::new(static_ref)
            } else {
                let rdr = Box::new(
                    CSVReader::new(node_path.clone(), edge_path.clone())
                        .with_separator(&separator)
                        .headers(has_headers)
                        .flexible(is_flexible),
                );
                let static_ref = Box::leak(rdr);
                TextGraphReader::new(static_ref)
            }
        } else {
            let rdr = Box::new(CSVReader::default());
            let static_ref = Box::leak(rdr);
            TextGraphReader::new(static_ref)
        };

        let mut partitioner = Partitioner::<_, _, _, Id, TimeStamp>::new(
            data_prefix.clone(),
            fs,
            Box::new(text_graph_reader),
            true,
            reorder_node_id,
            buffer,
            StaticGraph,
            full_rep,
            sync,
            num_of_machines,
        );

        partitioner.run(&mut worker).unwrap();
    })
    .unwrap();
}
