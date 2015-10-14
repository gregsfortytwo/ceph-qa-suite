"""
Teuthology task for running the CTDB ping pong test against ceph and making sure
it passes.
Note that right now ceph-fuse will require the page cache be disabled for this
to pass...
"""
from cStringIO import StringIO
import logging
import os
import time

from teuthology.orchestra import run
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestPingPong(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    ppfile = "pp_test_file.data"

    def setUp(self):
        CephFSTestCase.setUp(self)

        def build_pingpong(mount):
            mount.client_remote.run(args=[
                'cd', mount.test_dir,
                run.Raw('&&'), 'wget', 'http://download.ceph.com/qa/ping_pong.c',
                run.Raw('&&'), 'cc', '-o', 'ping_pong', 'ping_pong.c'
            ], wait=True)
            mount.pingpong_loc = os.path.join(mount.test_dir, 'ping_pong')

        build_pingpong(self.mount_a)
        build_pingpong(self.mount_b)
        self.pp_num = str(int(self.CLIENTS_REQUIRED) + 1)
            
    def test_io_coherence(self):
        # Check that running the IO coherence test outputs the right values

        self.mount_a.open_no_data(self.ppfile)
        
        def invoke_pingpong(mount, target):
            p = mount.client_remote.run(
                args=[
                    'daemon_helper', 'kill',
                    "./{execf}".format(execf=mount.pingpong_loc), '-rw', target, self.pp_num
                ],
                wait=False,
                stdin=run.PIPE #,
                #stdout=StringIO()
            )
            return p

        # start ping_pong on each client
        mount_a_pp = invoke_pingpong(self.mount_a, self.ppfile)
        mount_b_pp = invoke_pingpong(self.mount_b, self.ppfile)

        # let it run long enough to actually do something
        time.sleep(10)
        mount_a_pp.stdin.close()
        mount_b_pp.stdin.close()

        # make sure the output indicates the clients are coherent
