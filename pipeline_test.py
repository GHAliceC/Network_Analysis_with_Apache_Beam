import apache_beam as beam
from apache_beam.testing import test_pipeline
from apache_beam.testing import util as beam_test_util

from google3.experimental.users.zhenyuzhou.beam import pipeline
from google3.experimental.users.zhenyuzhou.beam import schema
from google3.testing.pybase import googletest
from google3.testing.pybase import parameterized


class PipelineTest(parameterized.TestCase):

  def test_single_db_counts_only_up_to_down_and_ignores_unrelated_updates(self):
    diffs = [
        # Base snapshot at t=0: eth0 is UP, eth1 is DOWN, version is v1.0.
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=0.0,
            commit_id='c0',
            author='init_daemon',
            switch_role='TOR',
            is_base_snapshot=True,
            binary_version=schema.BinaryVersionDiff(
                version='v1.0',
                build_cl=900000001,
                build_timestamp=1700000000.0,
                package_name='orion_nib_server',
            ),
            port_diffs=[
                schema.PortDiff(
                    port_name='eth0',
                    port_id=1,
                    status=schema.PortStatus.UP,
                    admin_status=schema.PortStatus.UP,
                    speed_gbps=400,
                    mtu=9100,
                    mac_address='00:1a:11:00:00:01',
                    description='uplink-to-spine1',
                    fec_mode='RS_544_514',
                    lanes=(0, 1, 2, 3),
                    attributes={'optics_vendor': 'finisar'},
                ),
                schema.PortDiff(
                    port_name='eth1',
                    port_id=2,
                    status=schema.PortStatus.DOWN,
                    admin_status=schema.PortStatus.UP,
                    speed_gbps=100,
                    mtu=9100,
                    mac_address='00:1a:11:00:00:02',
                ),
            ],
            vlan_diffs=[
                schema.VlanDiff(
                    vlan_id=100,
                    name='prod_vlan',
                    member_ports=('eth0', 'eth1'),
                )
            ],
            bgp_neighbor_diffs=[
                schema.BgpNeighborDiff(
                    neighbor_ip='192.0.2.1',
                    peer_asn=65001,
                    session_state='ESTABLISHED',
                    vrf_name='default',
                )
            ],
            route_diffs=[
                schema.RouteDiff(
                    prefix='10.0.0.0/8',
                    next_hop_group_id=10,
                    protocol='BGP',
                    metric=20,
                )
            ],
            other_tables={'ACL_RULE': [{'rule_id': 1, 'action': 'PERMIT'}]},
        ),
        # Pre-window diff at t=50: eth0 goes DOWN (before start_time=100),
        # eth1 goes UP.
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=50.0,
            commit_id='c1',
            parent_commit_id='c0',
            port_diffs=[
                schema.PortDiff(
                    port_name='eth0', status=schema.PortStatus.DOWN
                ),
                schema.PortDiff(port_name='eth1', status=schema.PortStatus.UP),
            ],
        ),
        # In-window diff at t=110: touches only BGP_NEIGHBOR, ROUTE, and non-status
        # PORT columns (status=None). Should be completely ignored.
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=110.0,
            commit_id='c2',
            parent_commit_id='c1',
            author='teapp',
            port_diffs=[
                schema.PortDiff(
                    port_name='eth1',
                    mtu=9216,
                    description='updated-mtu-only',
                ),
            ],
            bgp_neighbor_diffs=[
                schema.BgpNeighborDiff(
                    neighbor_ip='192.0.2.2',
                    peer_asn=65002,
                    session_state='ACTIVE',
                )
            ],
            route_diffs=[
                schema.RouteDiff(
                    prefix='172.16.0.0/12',
                    next_hop_group_id=11,
                    protocol='STATIC',
                )
            ],
        ),
        # In-window diff at t=120:
        # - eth0 stays DOWN (attribute change only -> NOT counted)
        # - eth1 transitions UP -> DOWN (counted: #1)
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=120.0,
            commit_id='c3',
            parent_commit_id='c2',
            port_diffs=[
                schema.PortDiff(
                    port_name='eth0',
                    status=schema.PortStatus.DOWN,
                    description='still down',
                ),
                schema.PortDiff(
                    port_name='eth1', status=schema.PortStatus.DOWN
                ),
            ],
        ),
        # In-window diff at t=150: eth1 transitions DOWN -> UP (NOT counted).
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=150.0,
            commit_id='c4',
            parent_commit_id='c3',
            port_diffs=[
                schema.PortDiff(port_name='eth1', status=schema.PortStatus.UP),
            ],
        ),
        # In-window diff at t=180: eth1 transitions UP -> DOWN again (counted: #2).
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=180.0,
            commit_id='c5',
            parent_commit_id='c4',
            port_diffs=[
                schema.PortDiff(
                    port_name='eth1', status=schema.PortStatus.DOWN
                ),
            ],
        ),
        # Post-window diff at t=250 (after end_time=200 -> pruned).
        schema.DatabaseDiff(
            db_id='db_1',
            timestamp=250.0,
            commit_id='c6',
            parent_commit_id='c5',
            port_diffs=[
                schema.PortDiff(port_name='eth0', status=schema.PortStatus.UP),
                schema.PortDiff(
                    port_name='eth0', status=schema.PortStatus.DOWN
                ),
            ],
        ),
    ]

    with test_pipeline.TestPipeline() as p:
      results = (
          p
          | beam.Create(diffs)
          | pipeline.ComputePortDownRates(
              target_version='v1.0',
              start_time=100.0,
              end_time=200.0,
          )
      )
      beam_test_util.assert_that(
          results,
          beam_test_util.equal_to([
              schema.PortDownRateResult(
                  db_id='db_1',
                  port_down_rate=2 / 100.0,
                  port_down_count=2,
                  active_time_seconds=100.0,
              )
          ]),
      )

  def test_mid_window_version_upgrade_normalizes_by_active_time(self):
    # Query window is [100, 200], target_version is 'v2.0'.
    # - t=0..140: running 'v1.0' (40s inside window on v1.0)
    # - t=140..190: running 'v2.0' (50s inside window on v2.0)
    # - t=190..200: running 'v3.0' (10s inside window on v3.0)
    diffs = [
        schema.DatabaseDiff(
            db_id='db_upgrade',
            timestamp=0.0,
            is_base_snapshot=True,
            binary_version=schema.BinaryVersionDiff(version='v1.0'),
            port_diffs=[
                schema.PortDiff(port_name='p1', status=schema.PortStatus.UP),
                schema.PortDiff(port_name='p2', status=schema.PortStatus.UP),
                schema.PortDiff(port_name='p3', status=schema.PortStatus.UP),
            ],
        ),
        # At t=120 (still on v1.0): p1 goes UP -> DOWN (ignored, wrong version).
        schema.DatabaseDiff(
            db_id='db_upgrade',
            timestamp=120.0,
            port_diffs=[
                schema.PortDiff(port_name='p1', status=schema.PortStatus.DOWN),
            ],
        ),
        # At t=140: upgrades to v2.0 and p2 goes UP -> DOWN in the same diff.
        schema.DatabaseDiff(
            db_id='db_upgrade',
            timestamp=140.0,
            binary_version=schema.BinaryVersionDiff(version='v2.0'),
            port_diffs=[
                schema.PortDiff(port_name='p2', status=schema.PortStatus.DOWN),
            ],
        ),
        # At t=160 (on v2.0): p3 goes UP -> DOWN.
        schema.DatabaseDiff(
            db_id='db_upgrade',
            timestamp=160.0,
            port_diffs=[
                schema.PortDiff(port_name='p3', status=schema.PortStatus.DOWN),
            ],
        ),
        # At t=190: upgrades to v3.0 and p1 goes UP -> DOWN (ignored).
        schema.DatabaseDiff(
            db_id='db_upgrade',
            timestamp=190.0,
            binary_version=schema.BinaryVersionDiff(version='v3.0'),
            port_diffs=[
                schema.PortDiff(port_name='p1', status=schema.PortStatus.UP),
                schema.PortDiff(port_name='p1', status=schema.PortStatus.DOWN),
            ],
        ),
    ]

    with test_pipeline.TestPipeline() as p:
      results = (
          p
          | beam.Create(diffs)
          | pipeline.ComputePortDownRates(
              target_version='v2.0',
              start_time=100.0,
              end_time=200.0,
          )
      )
      beam_test_util.assert_that(
          results,
          beam_test_util.equal_to([
              schema.PortDownRateResult(
                  db_id='db_upgrade',
                  port_down_rate=2 / 50.0,
                  port_down_count=2,
                  active_time_seconds=50.0,
              )
          ]),
      )

  def test_multiple_dbs_filters_unmatched_and_handles_same_port_ids(self):
    # Both db_a and db_b have ports with port_id=1, distinguished by port_name.
    # db_other never runs target_version='v1.0' during [100, 200].
    diffs = [
        schema.DatabaseDiff(
            db_id='db_a',
            timestamp=0.0,
            is_base_snapshot=True,
            binary_version=schema.BinaryVersionDiff(version='v1.0'),
            port_diffs=[
                schema.PortDiff(
                    port_name='sw1:port1',
                    port_id=1,
                    status=schema.PortStatus.UP,
                ),
                schema.PortDiff(
                    port_name='sw2:port1',
                    port_id=1,
                    status=schema.PortStatus.UP,
                ),
            ],
        ),
        schema.DatabaseDiff(
            db_id='db_a',
            timestamp=150.0,
            port_diffs=[
                schema.PortDiff(
                    port_name='sw1:port1',
                    port_id=1,
                    status=schema.PortStatus.DOWN,
                ),
            ],
        ),
        schema.DatabaseDiff(
            db_id='db_b',
            timestamp=0.0,
            is_base_snapshot=True,
            binary_version=schema.BinaryVersionDiff(version='v1.0'),
            port_diffs=[
                schema.PortDiff(
                    port_name='sw1:port1',
                    port_id=1,
                    status=schema.PortStatus.UP,
                ),
            ],
        ),
        schema.DatabaseDiff(
            db_id='db_other',
            timestamp=0.0,
            is_base_snapshot=True,
            binary_version=schema.BinaryVersionDiff(version='v0.9'),
            port_diffs=[
                schema.PortDiff(
                    port_name='sw1:port1',
                    port_id=1,
                    status=schema.PortStatus.UP,
                ),
            ],
        ),
        schema.DatabaseDiff(
            db_id='db_other',
            timestamp=150.0,
            port_diffs=[
                schema.PortDiff(
                    port_name='sw1:port1',
                    port_id=1,
                    status=schema.PortStatus.DOWN,
                ),
            ],
        ),
    ]

    with test_pipeline.TestPipeline() as p:
      results = (
          p
          | beam.Create(diffs)
          | pipeline.ComputePortDownRates(
              target_version='v1.0',
              start_time=100.0,
              end_time=200.0,
          )
      )
      beam_test_util.assert_that(
          results,
          beam_test_util.equal_to([
              schema.PortDownRateResult(
                  db_id='db_a',
                  port_down_rate=1 / 100.0,
                  port_down_count=1,
                  active_time_seconds=100.0,
              ),
              schema.PortDownRateResult(
                  db_id='db_b',
                  port_down_rate=0.0,
                  port_down_count=0,
                  active_time_seconds=100.0,
              ),
          ]),
      )

  @parameterized.named_parameters(
      dict(
          testcase_name='empty_target_version',
          target_version='',
          start_time=0.0,
          end_time=100.0,
          expected_error='target_version must be a non-empty string',
      ),
      dict(
          testcase_name='end_time_equal_to_start_time',
          target_version='v1.0',
          start_time=100.0,
          end_time=100.0,
          expected_error='must be strictly greater than start_time',
      ),
      dict(
          testcase_name='end_time_less_than_start_time',
          target_version='v1.0',
          start_time=200.0,
          end_time=100.0,
          expected_error='must be strictly greater than start_time',
      ),
  )
  def test_invalid_transform_arguments_raises_value_error(
      self,
      target_version: str,
      start_time: float,
      end_time: float,
      expected_error: str,
  ):
    with self.assertRaisesRegex(ValueError, expected_error):
      pipeline.ComputePortDownRates(
          target_version=target_version,
          start_time=start_time,
          end_time=end_time,
      )

  def test_end_to_end_jsonl_file_pipeline(self):
    raw_records = [
        schema.DatabaseDiff(
            db_id='db_json',
            timestamp=10.0,
            commit_id='commit_0',
            author='dpap',
            switch_role='SPINE',
            is_base_snapshot=True,
            binary_version=schema.BinaryVersionDiff(
                version='v1.2',
                build_cl=950000000,
                build_timestamp=1710000000.0,
                package_name='orion_nib',
                attributes={'release_channel': 'canary'},
            ),
            port_diffs=[
                schema.PortDiff(
                    port_name='Ethernet1/1',
                    port_id=10,
                    status=schema.PortStatus.UP,
                    admin_status=schema.PortStatus.UP,
                    speed_gbps=800,
                    mtu=9100,
                    mac_address='00:1a:11:aa:bb:cc',
                    description='fabric-link-1',
                    fec_mode='RS_544_514',
                    lanes=(0, 1, 2, 3, 4, 5, 6, 7),
                    attributes={'transceiver': 'OSFP'},
                )
            ],
            vlan_diffs=[
                schema.VlanDiff(
                    vlan_id=200,
                    name='fabric_vlan',
                    member_ports=('Ethernet1/1',),
                )
            ],
            bgp_neighbor_diffs=[
                schema.BgpNeighborDiff(
                    neighbor_ip='192.0.2.1',
                    peer_asn=65100,
                    session_state='ESTABLISHED',
                )
            ],
            route_diffs=[
                schema.RouteDiff(
                    prefix='10.128.0.0/9',
                    next_hop_group_id=42,
                    protocol='BGP',
                    metric=10,
                )
            ],
            other_tables={'QOS_QUEUE': [{'queue_id': 3, 'weight': 50}]},
        ).to_json(),
        schema.DatabaseDiff(
            db_id='db_json',
            timestamp=125.0,
            commit_id='commit_1',
            parent_commit_id='commit_0',
            port_diffs=[
                schema.PortDiff(
                    port_name='Ethernet1/1',
                    port_id=10,
                    status=schema.PortStatus.DOWN,
                    admin_status=schema.PortStatus.UP,
                )
            ],
        ).to_json(),
    ]
    input_file = self.create_tempfile(
        'input.jsonl', content='\n'.join(raw_records)
    )
    output_dir = self.create_tempdir('output')
    output_file = f'{output_dir.full_path}/results.jsonl'

    pipeline.run_pipeline(
        input_path=input_file.full_path,
        output_path=output_file,
        target_version='v1.2',
        start_time=100.0,
        end_time=150.0,
        beam_args=['--runner=DirectRunner'],
    )

    with open(output_file, 'rt') as f:
      lines = [line.strip() for line in f if line.strip()]
    self.assertSequenceEqual(
        lines,
        [
            schema.PortDownRateResult(
                db_id='db_json',
                port_down_rate=1 / 50.0,
                port_down_count=1,
                active_time_seconds=50.0,
            ).to_json()
        ],
    )


if __name__ == '__main__':
  googletest.main()
