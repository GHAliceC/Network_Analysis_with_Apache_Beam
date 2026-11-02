"""Extensible data schema for networking database diffs.

This module models incremental diffs written to a networking database, where
each `DatabaseDiff` acts like a git commit applied atop the preceding snapshot:
  - The first diff for a `db_id` (`is_base_snapshot=True`) represents the
    initial snapshot from an empty state.
  - Subsequent diffs contain only the tables and columns modified in that
    transaction; any omitted table or column retains its value from the
    previous snapshot.
  - Known tables used by the port-down pipeline (`PORT` and `BINARY_VERSION`)
    and common unrelated networking tables (`VLAN`, `BGP_NEIGHBOR`, `ROUTE`)
    are modeled as frozen dataclasses, while arbitrary future columns and
    tables are preserved via `attributes` and `other_tables`.
"""

from collections.abc import Mapping, Sequence
import dataclasses
import enum
import json
from typing import Any


class PortStatus(enum.Enum):
  """Operational or administrative status of a networking port."""

  UP = 'UP'
  DOWN = 'DOWN'

  @classmethod
  def from_raw(cls, value: 'PortStatus | str') -> 'PortStatus':
    """Parses a `PortStatus` from an enum instance or case-insensitive string.

    Args:
      value: A `PortStatus` instance or string representation ('UP' or 'DOWN').

    Returns:
      The corresponding `PortStatus` enum member.

    Raises:
      ValueError: If `value` does not match a valid `PortStatus`.
    """
    if isinstance(value, cls):
      return value
    normalized = value.strip().upper()
    try:
      return cls(normalized)
    except ValueError as err:
      raise ValueError(f'Unsupported port status: {value!r}.') from err


@dataclasses.dataclass(frozen=True)
class PortDiff:
  """Represents a row update in the `PORT` table.

  `port_name` is the primary key of the `PORT` table. `port_id` is a numeric
  identifier that is not unique across switches. Only `port_name` and `status`
  are needed to detect `UP -> DOWN` transitions; all other columns represent
  realistic port configuration/state fields that are pruned prior to shuffle.

  Attributes:
    port_name: Primary key identifying the port within a database.
    port_id: Optional numeric port identifier (not unique across switches).
    status: Updated operational status of the port, or `None` if unmodified.
    admin_status: Administrative state of the port, or `None` if unmodified.
    speed_gbps: Configured port speed in Gbps (e.g., 100, 400, 800).
    mtu: Maximum transmission unit in bytes (e.g., 9100).
    mac_address: Assigned hardware MAC address string.
    description: Human-readable interface description or peer link label.
    fec_mode: Forward Error Correction mode (e.g., `'RS_544_514'`).
    lanes: Tuple of physical serdes lane indices bound to this port.
    attributes: Additional extensible `PORT` column updates.
  """

  port_name: str
  port_id: int | None = None
  status: PortStatus | None = None
  admin_status: PortStatus | None = None
  speed_gbps: int | None = None
  mtu: int | None = None
  mac_address: str | None = None
  description: str | None = None
  fec_mode: str | None = None
  lanes: tuple[int, ...] = ()
  attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

  @classmethod
  def from_dict(cls, raw_port: Mapping[str, Any]) -> 'PortDiff':
    """Constructs a `PortDiff` from a dictionary mapping.

    Args:
      raw_port: Dictionary containing at least `'port_name'`.

    Returns:
      A populated `PortDiff` instance.
    """
    raw_status = raw_port.get('status')
    status = PortStatus.from_raw(raw_status) if raw_status is not None else None
    raw_admin = raw_port.get('admin_status')
    admin_status = (
        PortStatus.from_raw(raw_admin) if raw_admin is not None else None
    )
    # Any unrecognized keys in `raw_port` are automatically captured into
    # `extra_attributes` so new `PORT` columns do not break deserialization.
    reserved_keys = {
        'port_name',
        'port_id',
        'status',
        'admin_status',
        'speed_gbps',
        'mtu',
        'mac_address',
        'description',
        'fec_mode',
        'lanes',
        'attributes',
    }
    extra_attributes = dict(raw_port.get('attributes', {}))
    for key, value in raw_port.items():
      if key not in reserved_keys:
        extra_attributes[key] = value
    return cls(
        port_name=str(raw_port['port_name']),
        port_id=(
            int(raw_port['port_id'])
            if raw_port.get('port_id') is not None
            else None
        ),
        status=status,
        admin_status=admin_status,
        speed_gbps=(
            int(raw_port['speed_gbps'])
            if raw_port.get('speed_gbps') is not None
            else None
        ),
        mtu=int(raw_port['mtu']) if raw_port.get('mtu') is not None else None,
        mac_address=(
            str(raw_port['mac_address'])
            if raw_port.get('mac_address') is not None
            else None
        ),
        description=(
            str(raw_port['description'])
            if raw_port.get('description') is not None
            else None
        ),
        fec_mode=(
            str(raw_port['fec_mode'])
            if raw_port.get('fec_mode') is not None
            else None
        ),
        lanes=tuple(int(lane) for lane in raw_port.get('lanes', ())),
        attributes=extra_attributes,
    )

  def to_dict(self) -> dict[str, Any]:
    """Serializes the `PortDiff` to a JSON-compatible dictionary."""
    result: dict[str, Any] = {'port_name': self.port_name}
    if self.port_id is not None:
      result['port_id'] = self.port_id
    if self.status is not None:
      result['status'] = self.status.value
    if self.admin_status is not None:
      result['admin_status'] = self.admin_status.value
    if self.speed_gbps is not None:
      result['speed_gbps'] = self.speed_gbps
    if self.mtu is not None:
      result['mtu'] = self.mtu
    if self.mac_address is not None:
      result['mac_address'] = self.mac_address
    if self.description is not None:
      result['description'] = self.description
    if self.fec_mode is not None:
      result['fec_mode'] = self.fec_mode
    if self.lanes:
      result['lanes'] = list(self.lanes)
    if self.attributes:
      result['attributes'] = dict(self.attributes)
    return result


@dataclasses.dataclass(frozen=True)
class BinaryVersionDiff:
  """Represents an update to the `BINARY_VERSION` table.

  Attributes:
    version: Version string of the database binary running at the diff time.
    build_cl: Optional changelist number from which the binary was built.
    build_timestamp: Optional epoch timestamp when the binary was built.
    package_name: Optional release package or MPM label.
    attributes: Additional extensible `BINARY_VERSION` metadata columns.
  """

  version: str
  build_cl: int | None = None
  build_timestamp: float | None = None
  package_name: str | None = None
  attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

  @classmethod
  def from_dict(
      cls, raw_version: Mapping[str, Any] | str
  ) -> 'BinaryVersionDiff':
    """Constructs a `BinaryVersionDiff` from a string or dictionary mapping.

    Args:
      raw_version: Either a version string or a mapping with a `'version'` key.

    Returns:
      A populated `BinaryVersionDiff` instance.
    """
    if isinstance(raw_version, str):
      return cls(version=raw_version)
    reserved_keys = {
        'version',
        'build_cl',
        'build_timestamp',
        'package_name',
        'attributes',
    }
    extra_attributes = dict(raw_version.get('attributes', {}))
    for key, value in raw_version.items():
      if key not in reserved_keys:
        extra_attributes[key] = value
    return cls(
        version=str(raw_version['version']),
        build_cl=(
            int(raw_version['build_cl'])
            if raw_version.get('build_cl') is not None
            else None
        ),
        build_timestamp=(
            float(raw_version['build_timestamp'])
            if raw_version.get('build_timestamp') is not None
            else None
        ),
        package_name=(
            str(raw_version['package_name'])
            if raw_version.get('package_name') is not None
            else None
        ),
        attributes=extra_attributes,
    )

  def to_dict(self) -> dict[str, Any]:
    """Serializes the `BinaryVersionDiff` to a JSON-compatible dictionary."""
    result: dict[str, Any] = {'version': self.version}
    if self.build_cl is not None:
      result['build_cl'] = self.build_cl
    if self.build_timestamp is not None:
      result['build_timestamp'] = self.build_timestamp
    if self.package_name is not None:
      result['package_name'] = self.package_name
    if self.attributes:
      result['attributes'] = dict(self.attributes)
    return result


@dataclasses.dataclass(frozen=True)
class VlanDiff:
  """Represents a row update in the `VLAN` table (unrelated to port-down rate).

  Attributes:
    vlan_id: Numeric 802.1Q VLAN identifier.
    name: Optional VLAN name.
    member_ports: Tuple of port names assigned to this VLAN.
  """

  vlan_id: int
  name: str | None = None
  member_ports: tuple[str, ...] = ()

  @classmethod
  def from_dict(cls, raw_vlan: Mapping[str, Any]) -> 'VlanDiff':
    """Constructs a `VlanDiff` from a dictionary mapping."""
    return cls(
        vlan_id=int(raw_vlan['vlan_id']),
        name=str(raw_vlan['name'])
        if raw_vlan.get('name') is not None
        else None,
        member_ports=tuple(
            str(port) for port in raw_vlan.get('member_ports', ())
        ),
    )

  def to_dict(self) -> dict[str, Any]:
    """Serializes the `VlanDiff` to a JSON-compatible dictionary."""
    result: dict[str, Any] = {'vlan_id': self.vlan_id}
    if self.name is not None:
      result['name'] = self.name
    if self.member_ports:
      result['member_ports'] = list(self.member_ports)
    return result


@dataclasses.dataclass(frozen=True)
class BgpNeighborDiff:
  """Represents a row update in the `BGP_NEIGHBOR` table.

  Attributes:
    neighbor_ip: Peer IPv4 or IPv6 address string.
    peer_asn: Remote Autonomous System Number.
    session_state: BGP session state (e.g., `'ESTABLISHED'`, `'ACTIVE'`).
    vrf_name: Virtual routing and forwarding instance name.
  """

  neighbor_ip: str
  peer_asn: int | None = None
  session_state: str | None = None
  vrf_name: str | None = None

  @classmethod
  def from_dict(cls, raw_neighbor: Mapping[str, Any]) -> 'BgpNeighborDiff':
    """Constructs a `BgpNeighborDiff` from a dictionary mapping."""
    return cls(
        neighbor_ip=str(raw_neighbor['neighbor_ip']),
        peer_asn=(
            int(raw_neighbor['peer_asn'])
            if raw_neighbor.get('peer_asn') is not None
            else None
        ),
        session_state=(
            str(raw_neighbor['session_state'])
            if raw_neighbor.get('session_state') is not None
            else None
        ),
        vrf_name=(
            str(raw_neighbor['vrf_name'])
            if raw_neighbor.get('vrf_name') is not None
            else None
        ),
    )

  def to_dict(self) -> dict[str, Any]:
    """Serializes the `BgpNeighborDiff` to a JSON-compatible dictionary."""
    result: dict[str, Any] = {'neighbor_ip': self.neighbor_ip}
    if self.peer_asn is not None:
      result['peer_asn'] = self.peer_asn
    if self.session_state is not None:
      result['session_state'] = self.session_state
    if self.vrf_name is not None:
      result['vrf_name'] = self.vrf_name
    return result


@dataclasses.dataclass(frozen=True)
class RouteDiff:
  """Represents a row update in the `ROUTE` table.

  Attributes:
    prefix: Destination IP prefix in CIDR notation.
    next_hop_group_id: Identifier of the resolved next-hop group.
    protocol: Routing protocol source (e.g., `'BGP'`, `'STATIC'`, `'ISIS'`).
    metric: Route preference metric or cost.
  """

  prefix: str
  next_hop_group_id: int | None = None
  protocol: str | None = None
  metric: int | None = None

  @classmethod
  def from_dict(cls, raw_route: Mapping[str, Any]) -> 'RouteDiff':
    """Constructs a `RouteDiff` from a dictionary mapping."""
    return cls(
        prefix=str(raw_route['prefix']),
        next_hop_group_id=(
            int(raw_route['next_hop_group_id'])
            if raw_route.get('next_hop_group_id') is not None
            else None
        ),
        protocol=(
            str(raw_route['protocol'])
            if raw_route.get('protocol') is not None
            else None
        ),
        metric=(
            int(raw_route['metric'])
            if raw_route.get('metric') is not None
            else None
        ),
    )

  def to_dict(self) -> dict[str, Any]:
    """Serializes the `RouteDiff` to a JSON-compatible dictionary."""
    result: dict[str, Any] = {'prefix': self.prefix}
    if self.next_hop_group_id is not None:
      result['next_hop_group_id'] = self.next_hop_group_id
    if self.protocol is not None:
      result['protocol'] = self.protocol
    if self.metric is not None:
      result['metric'] = self.metric
    return result


@dataclasses.dataclass(frozen=True)
class DatabaseDiff:
  """Represents a single commit-like diff written to a networking database.

  Attributes:
    db_id: Identifier of the networking database instance.
    timestamp: Epoch timestamp (in seconds) when the diff was written.
    commit_id: Unique hash or sequence identifier for this diff commit.
    parent_commit_id: Commit identifier of the preceding database state.
    author: Service or controller component that authored the transaction.
    switch_role: Role of the device hosting the DB (e.g., `'TOR'`, `'SPINE'`).
    port_diffs: Sequence of updates to the `PORT` table in this diff.
    binary_version: Update to the `BINARY_VERSION` table, or `None` if
      unmodified by this diff.
    vlan_diffs: Sequence of updates to the `VLAN` table in this diff.
    bgp_neighbor_diffs: Sequence of updates to the `BGP_NEIGHBOR` table.
    route_diffs: Sequence of updates to the `ROUTE` table.
    is_base_snapshot: Whether this diff is the initial base snapshot.
    other_tables: Extensible mapping from table name to row diff dictionaries
      for any additional unmodeled tables in the database.
  """

  db_id: str
  timestamp: float
  commit_id: str | None = None
  parent_commit_id: str | None = None
  author: str | None = None
  switch_role: str | None = None
  port_diffs: Sequence[PortDiff] = ()
  binary_version: BinaryVersionDiff | None = None
  vlan_diffs: Sequence[VlanDiff] = ()
  bgp_neighbor_diffs: Sequence[BgpNeighborDiff] = ()
  route_diffs: Sequence[RouteDiff] = ()
  is_base_snapshot: bool = False
  other_tables: Mapping[str, Sequence[Mapping[str, Any]]] = dataclasses.field(
      default_factory=dict
  )

  @classmethod
  def from_dict(cls, raw_diff: Mapping[str, Any]) -> 'DatabaseDiff':
    """Constructs a `DatabaseDiff` from a parsed dictionary.

    Supports both top-level table fields and a nested `'tables'` dictionary
    containing `'PORT'`, `'BINARY_VERSION'`, `'VLAN'`, `'BGP_NEIGHBOR'`,
    `'ROUTE'`, and any future table names.

    Args:
      raw_diff: Mapping representing a single database diff record.

    Returns:
      A populated `DatabaseDiff` instance.
    """
    # Pop known tables from `'tables'` if present, and preserve any remaining
    # table entries in `other_tables` for forward compatibility.
    tables = dict(raw_diff.get('tables', {}))
    raw_ports = raw_diff.get('port_diffs', tables.pop('PORT', ()))
    port_diffs = tuple(PortDiff.from_dict(port) for port in raw_ports)

    raw_version = raw_diff.get(
        'binary_version', tables.pop('BINARY_VERSION', None)
    )
    binary_version = (
        BinaryVersionDiff.from_dict(raw_version)
        if raw_version is not None
        else None
    )

    raw_vlans = raw_diff.get('vlan_diffs', tables.pop('VLAN', ()))
    vlan_diffs = tuple(VlanDiff.from_dict(vlan) for vlan in raw_vlans)

    raw_neighbors = raw_diff.get(
        'bgp_neighbor_diffs', tables.pop('BGP_NEIGHBOR', ())
    )
    bgp_neighbor_diffs = tuple(
        BgpNeighborDiff.from_dict(neighbor) for neighbor in raw_neighbors
    )

    raw_routes = raw_diff.get('route_diffs', tables.pop('ROUTE', ()))
    route_diffs = tuple(RouteDiff.from_dict(route) for route in raw_routes)

    other_tables: dict[str, Sequence[Mapping[str, Any]]] = {}
    for table_name, rows in raw_diff.get('other_tables', {}).items():
      other_tables[table_name] = tuple(dict(row) for row in rows)
    for table_name, rows in tables.items():
      other_tables[table_name] = tuple(dict(row) for row in rows)

    return cls(
        db_id=str(raw_diff['db_id']),
        timestamp=float(raw_diff['timestamp']),
        commit_id=(
            str(raw_diff['commit_id'])
            if raw_diff.get('commit_id') is not None
            else None
        ),
        parent_commit_id=(
            str(raw_diff['parent_commit_id'])
            if raw_diff.get('parent_commit_id') is not None
            else None
        ),
        author=(
            str(raw_diff['author'])
            if raw_diff.get('author') is not None
            else None
        ),
        switch_role=(
            str(raw_diff['switch_role'])
            if raw_diff.get('switch_role') is not None
            else None
        ),
        port_diffs=port_diffs,
        binary_version=binary_version,
        vlan_diffs=vlan_diffs,
        bgp_neighbor_diffs=bgp_neighbor_diffs,
        route_diffs=route_diffs,
        is_base_snapshot=bool(raw_diff.get('is_base_snapshot', False)),
        other_tables=other_tables,
    )

  @classmethod
  def from_json(cls, json_line: str) -> 'DatabaseDiff':
    """Deserializes a `DatabaseDiff` from a JSON string.

    Args:
      json_line: JSON-encoded string representing a single database diff.

    Returns:
      A populated `DatabaseDiff` instance.
    """
    return cls.from_dict(json.loads(json_line))

  def to_dict(self) -> dict[str, Any]:
    """Serializes the `DatabaseDiff` to a JSON-compatible dictionary."""
    tables: dict[str, Any] = {}
    if self.port_diffs:
      tables['PORT'] = [port.to_dict() for port in self.port_diffs]
    if self.binary_version is not None:
      tables['BINARY_VERSION'] = self.binary_version.to_dict()
    if self.vlan_diffs:
      tables['VLAN'] = [vlan.to_dict() for vlan in self.vlan_diffs]
    if self.bgp_neighbor_diffs:
      tables['BGP_NEIGHBOR'] = [
          neighbor.to_dict() for neighbor in self.bgp_neighbor_diffs
      ]
    if self.route_diffs:
      tables['ROUTE'] = [route.to_dict() for route in self.route_diffs]
    for table_name, rows in self.other_tables.items():
      tables[table_name] = [dict(row) for row in rows]

    result: dict[str, Any] = {
        'db_id': self.db_id,
        'timestamp': self.timestamp,
        'is_base_snapshot': self.is_base_snapshot,
        'tables': tables,
    }
    if self.commit_id is not None:
      result['commit_id'] = self.commit_id
    if self.parent_commit_id is not None:
      result['parent_commit_id'] = self.parent_commit_id
    if self.author is not None:
      result['author'] = self.author
    if self.switch_role is not None:
      result['switch_role'] = self.switch_role
    return result

  def to_json(self) -> str:
    """Serializes the `DatabaseDiff` to a compact JSON string."""
    return json.dumps(self.to_dict(), sort_keys=True)


@dataclasses.dataclass(frozen=True)
class PortDownRateResult:
  """Output record containing the normalized port-down rate for a database.

  Attributes:
    db_id: Identifier of the networking database instance.
    port_down_rate: Rate of `UP -> DOWN` port transitions per unit of active
      time spent on the target binary version.
    port_down_count: Total qualifying `UP -> DOWN` transitions in the window.
    active_time_seconds: Total time (in seconds) within the query window that
      the database ran the target binary version.
  """

  db_id: str
  port_down_rate: float
  port_down_count: int
  active_time_seconds: float

  def to_dict(self) -> dict[str, Any]:
    """Serializes the result to a JSON-compatible dictionary."""
    return dataclasses.asdict(self)

  def to_json(self) -> str:
    """Serializes the result to a compact JSON string."""
    return json.dumps(self.to_dict(), sort_keys=True)
