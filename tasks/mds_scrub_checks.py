"""
MDS admin socket scrubbing-related tests.
"""
import json
import logging

from teuthology.orchestra import run
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run flush and scrub commands on the specified files in the filesystem. This
    task will run through a sequence of operations, but it is not comprehensive
    on its own -- it doesn't manipulate the mds cache state to test on both
    in- and out-of-memory parts of the hierarchy. So it's designed to be run
    multiple times within a single test run, so that the test can manipulate
    memory state.

    Usage:
    mds_scrub_checks:
      mds_id: a
      path: path/to/test/dir
      client: 0
      run_seq: [0-9]+

    Increment the run_seq on subsequent invocations within a single test run; it
    uses that value to generate unique folder and file names.
    """

    mds_id = config.get("mds_id")
    test_path = config.get("path")
    run_seq = config.get("run_seq")
    client_id = config.get("client")

    if mds_id is None or test_path is None or run_seq is None:
        raise ValueError("Must specify each of mds_id, test_path, run_seq, client_id in config!")

    teuthdir = teuthology.get_testdir(ctx)
    client_path = "{teuthdir}/mnt.{id_}/{test_path}".format(teuthdir=teuthdir,
                                                            id_=client_id,
                                                            test_path=test_path)

    log.info("Cloning repo into place (if not present)")
    repo_path = clone_repo(ctx, client_id, client_path)

    log.info("Initiating mds_scrub_checks on mds.{id_}, test_path {path}, run_seq {seq}".format(
            id_=mds_id, path=test_path, seq=run_seq))

    def json_validater(json,rc,element,expected_value):
        if (rc != 0):
            return False, "asok command returned error {rc}".format(rc=str(rc))
        element_value = json.get(element)
        if element_value != expected_value:
            return False, "unexpectedly got {jv} instead of {ev}!".format(
                jv=element_value,ev=expected_value)
        return True, "Succeeded"

    nep = "{test_path}/i/dont/exist".format(test_path=test_path)
    command = "flush_path {nep}".format(nep=nep)
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",-2))
    
    command = "scrub_path {nep}".format(nep=nep)
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",-2))

    test_repo_path = "{test_path}/ceph-qa-suite".format(test_path=test_path)
    dirpath = "{repo_path}/suites".format(repo_path=test_repo_path)

    if (run_seq == 0):
        log.info("First run: flushing {dirpath}".format(dirpath=dirpath))
        command = "flush_path {dirpath}".format(dirpath=dirpath)
        asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))
    command = "scrub_path {dirpath}".format(dirpath=dirpath)
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))
    
    filepath = "{repo_path}/suites/fs/verify/validater/valgrind.yaml".format(
        repo_path=test_repo_path)
    if (run_seq == 0):
        log.info("First run: flushing {filepath}".format(filepath=filepath))
        command = "flush_path {filepath}".format(filepath=filepath)
        asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))
    command = "scrub_path {filepath}".format(filepath=filepath)
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))
    
    if (run_seq == 0):
        log.info("First run: flushing base dir /")
        command = "flush_path /"
        asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))
    command = "scrub_path /"
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))

    client = ctx.manager.find_remote("client", client_id)
    new_dir = "{repo_path}/new_dir_{i}".format(repo_path=repo_path,i=run_seq)
    test_new_dir = "{repo_path}/new_dir_{i}".format(repo_path=test_repo_path,i=run_seq)
    client.run(args=[
            "mkdir", new_dir])
    command = "flush_path {dir}".format(dir=test_new_dir)
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))

    new_file = "{repo_path}/new_file_{i}".format(repo_path=repo_path,i=run_seq)
    test_new_file = "{repo_path}/new_file_{i}".format(repo_path=test_repo_path,i=run_seq)
    client.run(args=[
            "echo", "hello", run.Raw('>'), new_file])
    command = "flush_path {file}".format(file=test_new_file)
    asok_command(ctx, mds_id, command, lambda j,r:json_validater(j,r,"return_code",0))

    command = "flush_path /"
    asok_command(ctx, mds_id, command, lambda j,r: json_validater(j,r,"return_code",0))
    
class AsokCommandFailedError(Exception):
    """
    Exception thrown when we get an unexpected response on an admin socket command
    """
    def __init__(self, command, rc, json, errstring):
        self.command = command
        self.rc = rc
        self.json = json
        self.errstring = errstring

    def __str__(self):
        return "Admin socket: {command} failed with rc={rc},json output={json}, because '{es}'".format(
            command=self.command,rc=self.rc,json=self.json,es=self.errstring)
        

def asok_command(ctx, mds_id, command, validater):
    log.info("Running command '{command}'".format(command=command))
    
    command_list = command.split()

    proc = ctx.manager.admin_socket('mds', mds_id, command_list, check_status=False)
    rout = proc.exitstatus
    sout = proc.stdout.getvalue()

    if sout.strip():
        jout = json.loads(sout)
    else:
        jout = None

    log.info("command '{command}' got response code '{rout}' and stdout '{sout}'".format(
            command=command,rout=rout,sout=sout))

    success,errstring = validater(jout,rout)

    if not success:
        raise AsokCommandFailedError(command, rout, jout, errstring)

    return jout

def clone_repo(ctx, client_id, path):
    repo = "ceph-qa-suite"
    repo_path = "{path}/{repo}".format(path=path,repo=repo)

    client = ctx.manager.find_remote("client", client_id)
    client.run(
        args=[
            "mkdir", "-p", path
            ]
        )
    client.run(
        args=[
            "ls", repo_path, run.Raw('||'),
            "git", "clone", "http://github.com/ceph/{repo}".format(repo=repo),
            "{path}/{repo}".format(path=path,repo=repo)
            ]
        )

    return repo_path
