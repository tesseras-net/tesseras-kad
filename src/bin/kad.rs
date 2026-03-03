//! CLI binary for the Kademlia DHT.
//!
//! Subcommands: `start`, `put`, `get`.

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::thread;

fn usage() -> ! {
    eprintln!(
        "Usage:\n  \
         kad start -p PORT [-b HOST:PORT] [-i]\n  \
         kad put -b HOST:PORT KEY VALUE\n  \
         kad get -b HOST:PORT KEY"
    );
    process::exit(1);
}

fn parse_args() -> Command {
    let args: Vec<String> =
        std::env::args().collect();
    if args.len() < 2 {
        usage();
    }

    match args[1].as_str() {
        "start" => parse_start(&args[2..]),
        "put" => parse_put(&args[2..]),
        "get" => parse_get(&args[2..]),
        _ => usage(),
    }
}

fn parse_start(args: &[String]) -> Command {
    let mut port: Option<u16> = None;
    let mut bootstrap: Option<SocketAddr> = None;
    let mut interactive = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-p" => {
                i += 1;
                port = Some(
                    args.get(i)
                        .unwrap_or_else(|| {
                            eprintln!(
                                "missing port"
                            );
                            process::exit(1);
                        })
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!(
                                "invalid port"
                            );
                            process::exit(1);
                        }),
                );
            }
            "-b" => {
                i += 1;
                bootstrap = Some(
                    args.get(i)
                        .unwrap_or_else(|| {
                            eprintln!(
                                "missing bootstrap \
                                 address"
                            );
                            process::exit(1);
                        })
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!(
                                "invalid bootstrap \
                                 address"
                            );
                            process::exit(1);
                        }),
                );
            }
            "-i" => interactive = true,
            _ => usage(),
        }
        i += 1;
    }

    let port = port.unwrap_or_else(|| {
        eprintln!("port required (-p)");
        process::exit(1);
    });

    Command::Start {
        port,
        bootstrap,
        interactive,
    }
}

fn parse_put(args: &[String]) -> Command {
    let mut bootstrap: Option<SocketAddr> = None;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-b" => {
                i += 1;
                bootstrap = Some(
                    args.get(i)
                        .unwrap_or_else(|| {
                            eprintln!(
                                "missing bootstrap \
                                 address"
                            );
                            process::exit(1);
                        })
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!(
                                "invalid bootstrap \
                                 address"
                            );
                            process::exit(1);
                        }),
                );
            }
            _ => {
                if key.is_none() {
                    key = Some(args[i].clone());
                } else if value.is_none() {
                    value = Some(args[i].clone());
                } else {
                    usage();
                }
            }
        }
        i += 1;
    }

    let bootstrap = bootstrap.unwrap_or_else(|| {
        eprintln!("bootstrap required (-b)");
        process::exit(1);
    });
    let key = key.unwrap_or_else(|| {
        eprintln!("key required");
        process::exit(1);
    });
    let value = value.unwrap_or_else(|| {
        eprintln!("value required");
        process::exit(1);
    });

    Command::Put {
        bootstrap,
        key,
        value,
    }
}

fn parse_get(args: &[String]) -> Command {
    let mut bootstrap: Option<SocketAddr> = None;
    let mut key: Option<String> = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-b" => {
                i += 1;
                bootstrap = Some(
                    args.get(i)
                        .unwrap_or_else(|| {
                            eprintln!(
                                "missing bootstrap \
                                 address"
                            );
                            process::exit(1);
                        })
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!(
                                "invalid bootstrap \
                                 address"
                            );
                            process::exit(1);
                        }),
                );
            }
            _ => {
                if key.is_none() {
                    key = Some(args[i].clone());
                } else {
                    usage();
                }
            }
        }
        i += 1;
    }

    let bootstrap = bootstrap.unwrap_or_else(|| {
        eprintln!("bootstrap required (-b)");
        process::exit(1);
    });
    let key = key.unwrap_or_else(|| {
        eprintln!("key required");
        process::exit(1);
    });

    Command::Get { bootstrap, key }
}

enum Command {
    Start {
        port: u16,
        bootstrap: Option<SocketAddr>,
        interactive: bool,
    },
    Put {
        bootstrap: SocketAddr,
        key: String,
        value: String,
    },
    Get {
        bootstrap: SocketAddr,
        key: String,
    },
}

fn main() {
    let cmd = parse_args();

    match cmd {
        Command::Start {
            port,
            bootstrap,
            interactive,
        } => {
            run_start(port, bootstrap, interactive);
        }
        Command::Put {
            bootstrap,
            key,
            value,
        } => {
            run_put(bootstrap, &key, &value);
        }
        Command::Get { bootstrap, key } => {
            run_get(bootstrap, &key);
        }
    }
}

fn run_start(
    port: u16,
    bootstrap: Option<SocketAddr>,
    interactive: bool,
) {
    let addr: SocketAddr =
        format!("0.0.0.0:{port}")
            .parse()
            .unwrap();
    let node = kad::Node::new(addr)
        .unwrap_or_else(|e| {
            eprintln!("failed to start: {e}");
            process::exit(1);
        });

    println!(
        "Node {} listening on {}",
        node.id,
        node.local_addr().unwrap()
    );

    if let Some(boot_addr) = bootstrap {
        println!(
            "Bootstrapping via {boot_addr}..."
        );
        node.join(boot_addr).unwrap_or_else(|e| {
            eprintln!(
                "bootstrap failed: {e}"
            );
            process::exit(1);
        });
        println!("Joined network.");
    }

    if interactive {
        let bg_node = Arc::clone(&node);
        thread::spawn(move || bg_node.run());
        run_repl(&node);
    } else {
        node.run();
    }
}

fn run_put(
    bootstrap: SocketAddr,
    key: &str,
    value: &str,
) {
    let addr: SocketAddr =
        "127.0.0.1:0".parse().unwrap();
    let node = kad::Node::new(addr)
        .unwrap_or_else(|e| {
            eprintln!("failed to start: {e}");
            process::exit(1);
        });

    node.join(bootstrap).unwrap_or_else(|e| {
        eprintln!("bootstrap failed: {e}");
        process::exit(1);
    });

    let hash = kad::NodeId::from_sha1(key.as_bytes());
    node.store(hash, value.as_bytes().to_vec())
        .unwrap_or_else(|e| {
            eprintln!("store failed: {e}");
            process::exit(1);
        });

    println!("Stored '{key}' -> '{value}'");
}

fn run_get(bootstrap: SocketAddr, key: &str) {
    let addr: SocketAddr =
        "127.0.0.1:0".parse().unwrap();
    let node = kad::Node::new(addr)
        .unwrap_or_else(|e| {
            eprintln!("failed to start: {e}");
            process::exit(1);
        });

    node.join(bootstrap).unwrap_or_else(|e| {
        eprintln!("bootstrap failed: {e}");
        process::exit(1);
    });

    let hash = kad::NodeId::from_sha1(key.as_bytes());
    match node.find_value(hash) {
        Ok(val) => {
            match String::from_utf8(val) {
                Ok(s) => println!("{s}"),
                Err(e) => {
                    eprintln!(
                        "value is not UTF-8: {e}"
                    );
                    process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("get failed: {e}");
            process::exit(1);
        }
    }
}

fn run_repl(node: &Arc<kad::Node>) {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();

    loop {
        print!("kad> ");
        let _ = io::stdout().flush();
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => process::exit(0), // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("read error: {e}");
                process::exit(1);
            }
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (cmd, rest) = match trimmed
            .split_once(' ')
        {
            Some((c, r)) => (c, r.trim()),
            None => (trimmed, ""),
        };

        match cmd {
            "put" => cmd_put(node, rest),
            "get" => cmd_get(node, rest),
            "find" => cmd_find(node, rest),
            "ping" => cmd_ping(node, rest),
            "info" => cmd_info(node),
            "help" => cmd_help(),
            "quit" | "exit" => process::exit(0),
            _ => {
                eprintln!(
                    "unknown command: {cmd}"
                );
            }
        }
    }
}

fn cmd_put(node: &Arc<kad::Node>, args: &str) {
    let (key, value) =
        match args.split_once(' ') {
            Some((k, v)) => {
                (k.trim(), v.trim())
            }
            None => {
                eprintln!(
                    "usage: put KEY VALUE"
                );
                return;
            }
        };
    if key.is_empty() || value.is_empty() {
        eprintln!("usage: put KEY VALUE");
        return;
    }

    let hash =
        kad::NodeId::from_sha1(key.as_bytes());
    match node
        .store(hash, value.as_bytes().to_vec())
    {
        Ok(()) => {
            println!("Stored '{key}' -> '{value}'")
        }
        Err(e) => eprintln!("store failed: {e}"),
    }
}

fn cmd_get(node: &Arc<kad::Node>, args: &str) {
    let key = args.trim();
    if key.is_empty() {
        eprintln!("usage: get KEY");
        return;
    }

    let hash =
        kad::NodeId::from_sha1(key.as_bytes());
    match node.find_value(hash) {
        Ok(val) => match String::from_utf8(val) {
            Ok(s) => println!("{s}"),
            Err(e) => {
                eprintln!(
                    "value is not UTF-8: {e}"
                )
            }
        },
        Err(e) => eprintln!("get failed: {e}"),
    }
}

fn cmd_find(node: &Arc<kad::Node>, args: &str) {
    let key = args.trim();
    if key.is_empty() {
        eprintln!("usage: find KEY");
        return;
    }

    let hash =
        kad::NodeId::from_sha1(key.as_bytes());
    match node.find_node(hash) {
        Ok(contacts) => {
            if contacts.is_empty() {
                println!("No nodes found.");
            } else {
                for c in &contacts {
                    println!(
                        "  {} @ {}",
                        c.node_id, c.addr
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("find failed: {e}")
        }
    }
}

fn cmd_ping(node: &Arc<kad::Node>, args: &str) {
    let addr_str = args.trim();
    if addr_str.is_empty() {
        eprintln!("usage: ping HOST:PORT");
        return;
    }

    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!(
                "invalid address: {e}"
            );
            return;
        }
    };

    match node.ping(addr) {
        Ok(id) => println!("Pong from {id}"),
        Err(e) => eprintln!("ping failed: {e}"),
    }
}

fn cmd_info(node: &Arc<kad::Node>) {
    println!("Node ID:  {}", node.id);
    println!(
        "Address:  {}",
        node.local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| {
                "unknown".to_string()
            })
    );
    println!(
        "Contacts: {}",
        node.routing_table_size()
    );
    println!(
        "Stored:   {}",
        node.storage_count()
    );
}

fn cmd_help() {
    println!(
        "Commands:\n  \
         put KEY VALUE   Store a value\n  \
         get KEY         Retrieve a value\n  \
         find KEY        Find closest nodes\n  \
         ping HOST:PORT  Ping a remote node\n  \
         info            Show node info\n  \
         help            Show this help\n  \
         quit            Exit"
    );
}
