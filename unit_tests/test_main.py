__author__ = 'Chris Holcombe <chris.holcombe@canonical.com>'

"""
#[cfg(test)]
mod tests {
    use std.collections.BTreeMap
    use std.path.PathBuf

    use super.gluster.volume.{Brick, Transport, Volume, VolumeType}
    use super.gluster.peer.{Peer, State}
    use super.uuid.Uuid

    #[test]
    def test_all_peers_are_ready() {
        peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    },
                                    Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    }]
        ready = super.peers_are_ready(Ok(peers))
        println!("Peers are ready: {}", ready)
        assert!(ready)
    }

    #[test]
    def test_some_peers_are_ready() {
        peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.Connected,
                                    },
                                    Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    }]
        ready = super.peers_are_ready(Ok(peers))
        println!("Some peers are ready: {}", ready)
        assert_eq!(ready, False)
    }

    #[test]
    def test_find_new_peers() {
        peer1 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peer2 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }

        # peer1 and peer2 are in the cluster but only peer1 is actually serving a brick.
        # find_new_peers should return peer2 as a new peer
        peers: Vec<Peer> = vec![peer1.clone(), peer2.clone()]
        existing_brick = Brick {
            peer: peer1,
            path: PathBuf.from("/mnt/brick1"),
        }

        volume_info = Volume {
            name: "Test".to_string(),
            vol_type: VolumeType.Replicate,
            id: Uuid.new_v4(),
            status: "online".to_string(),
            transport: Transport.Tcp,
            bricks: vec![existing_brick],
            options: BTreeMap.new(),
        }
        new_peers = super.find_new_peers(peers, volume_info)
        assert_eq!(new_peers, vec![peer2])
    }

    #[test]
    def test_cartesian_product() {
        peer1 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peer2 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peers = vec![peer1.clone(), peer2.clone()]
        paths = vec!["/mnt/brick1".to_string(), "/mnt/brick2".to_string()]
        result = super.brick_and_server_cartesian_product(peers, paths)
        println!("brick_and_server_cartesian_product: {:}", result)
        assert_eq!(result,
                   vec![Brick {
                            peer: peer1.clone(),
                            path: PathBuf.from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf.from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer1.clone(),
                            path: PathBuf.from("/mnt/brick2"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf.from("/mnt/brick2"),
                        }])
    }
}
"""