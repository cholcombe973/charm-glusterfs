from enum import Enum
from ipaddress import ip_address
from typing import Optional
from result import Ok, Err, Result
import uuid
import xml.etree.ElementTree as etree

from gluster.lib import resolve_to_ip, run_command


# A enum representing the possible States that a Peer can be in
class State(Enum):
    Connected = "connected",
    Disconnected = "disconnected"
    Unknown = ""
    EstablishingConnection = "establishing connection"
    ProbeSentToPeer = "probe sent to peer"
    ProbeReceivedFromPeer = "probe received from peer"
    PeerInCluster = "peer in cluster"
    AcceptedPeerRequest = "accepted peer in cluster"
    SentAndReceivedPeerRequest = "sent and received peer request"
    PeerRejected = "peer rejected"
    PeerDetachInProgress = "peer detach in progress"
    ConnectedToPeer = "connected to peer"
    PeerIsConnectedAndAccepted = "peer is connected and accepted"
    InvalidState = "invalid state"

    def __str__(self) -> str:
        return "{}".format(self.value)

    @staticmethod
    def from_str(s: str):
        s = s.lower()
        if s == 'connected':
            return State.Connected
        elif s == 'disconnected':
            return State.Disconnected
        elif s == 'establishing connection':
            return State.EstablishingConnection
        elif s == 'probe sent to peer':
            return State.ProbeSentToPeer
        elif s == 'probe received from peer':
            return State.ProbeReceivedFromPeer
        elif s == 'peer in cluster':
            return State.PeerInCluster
        elif s == 'accepted peer in cluster':
            return State.AcceptedPeerRequest
        elif s == "sent and received peer request":
            return State.SentAndReceivedPeerRequest
        elif s == "peer rejected":
            return State.PeerRejected
        elif s == "peer detach in progress":
            return State.PeerDetachInProgress
        elif s == "connected to peer":
            return State.ConnectedToPeer
        elif s == "peer is connected and accepted":
            return State.PeerIsConnectedAndAccepted
        elif s == "invalid state":
            return State.InvalidState
        else:
            return None


class Peer(object):
    def __init__(self, uuid: uuid.UUID, hostname: ip_address,
                 status: Optional[State]):
        """
        A Gluster Peer.  A Peer is roughly equivalent to a server in Gluster.
        :param uuid: Unique identifier of this peer
        :param hostname: ip address of the peer
        :param status:  current State of the peer
        """
        self.uuid = uuid
        self.hostname = hostname
        self.status = status

    def __eq__(self, other):
        return self.uuid == other.uuid

    def __str__(self):
        return "UUID: {}  Hostname: {} Status: {}".format(
            self.uuid,
            self.hostname,
            self.status)


def get_peer(hostname: ip_address) -> Optional[Peer]:
    """
    This will query the Gluster peer list and return a Peer class for the peer
    :param hostname: String.  Name of the peer to get
    :return Peer or None in case of not found
    """
    peers = peer_list()
    if peers.is_err():
        return None

    for peer in peers.value:
        if peer.hostname == hostname:
            return peer
    return None


def parse_peer_status(output_xml: str) -> Result:
    """
    Take a peer status line and parse it into Peer objects
    :param output_xml: String.  THe peer status --xml output
    :return: Result containing either a list of peers or an Err
    """
    peers = []
    tree = etree.fromstring(output_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)

    peer_tree = tree.find('peerStatus')
    for status in peer_tree:
        peer_uuid = None
        hostname = None
        state = None
        if status.tag == 'peer':
            for peer_info in status:
                if peer_info.tag == 'uuid':
                    peer_uuid = uuid.UUID(peer_info.text)
                elif peer_info.tag == 'hostname':
                    hostname = ip_address(peer_info.text)
                elif peer_info.tag == 'stateStr':
                    state = State.from_str(peer_info.text)
            peers.append(Peer(uuid=peer_uuid,
                              hostname=hostname,
                              status=state))

    return Ok(peers)


def peer_status() -> Result:
    """
    Runs gluster peer status and returns a Vec<Peer> representing all the
    peers in the cluster
    Returns GlusterError if the command failed to run
    :return: List of Peers
    """
    arg_list = ["peer", "status", "--xml"]

    output = run_command("gluster", arg_list, True, False)
    if output.is_err():
        return Err(output.value)
    return parse_peer_status(output.value)


def peer_list() -> Result:
    """
    List all peers including localhost
    Runs gluster pool list and returns a Vec<Peer> representing all the peers
    in the cluster
    This also returns information for the localhost as a Peer.  peer_status()
    does not
    # Failures
    Returns GlusterError if the command failed to run
    """
    arg_list = ["pool", "list", "--xml"]
    output = run_command("gluster", arg_list, True, False)
    if output.is_err():
        return Err(output.value)
    return parse_peer_list(output.value)


def parse_peer_list(output_xml: str) -> Result:
    peers = []
    tree = etree.fromstring(output_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)

    peer_tree = tree.find('peerStatus')
    for status in peer_tree:
        peer_uuid = None
        hostname = None
        state = None
        if status.tag == 'peer':
            for peer_info in status:
                if peer_info.tag == 'uuid':
                    if peer_info.text is not None:
                        peer_uuid = uuid.UUID(peer_info.text)
                elif peer_info.tag == 'hostname':
                    if peer_info.text == "localhost":
                        resolve_result = resolve_to_ip("localhost")
                        if resolve_result.is_ok():
                            hostname = resolve_result.value
                        else:
                            return Err(
                                "Unable to resolve localhost to ip address")
                    else:
                        hostname = peer_info.text
                elif peer_info.tag == 'stateStr':
                    state = State.from_str(peer_info.text)
            peers.append(Peer(uuid=peer_uuid, hostname=hostname, status=state))

    return Ok(peers)


def peer_probe(hostname: str) -> Result:
    """
    Probe a peer and prevent double probing
    Adds a new peer to the cluster by hostname or ip address
    :param hostname: String.  Add a host to the cluster
    :return:
    """
    current_peers = peer_list()
    if current_peers.is_err():
        return Err(current_peers.value)
    for peer in current_peers.value:
        if peer.hostname is hostname:
            # Bail instead of double probing
            return Ok(0)  # Does it make sense to say this is ok?

    arg_list = ["peer", "probe", hostname]
    return run_command("gluster", arg_list, True, False)


def peer_remove(hostname: str, force: bool) -> Result:
    """
    Removes a peer from the cluster by hostname or ip address
    :param hostname: String.  Hostname to remove from the cluster
    :param force: bool.  Should the command be forced
    :return:
    """
    arg_list = ["peer", "detach", hostname]
    if force:
        arg_list.append("force")

    return run_command("gluster", arg_list, True, False)
