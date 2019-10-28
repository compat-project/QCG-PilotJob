import pytest
import json

from qcg.appscheduler.joblist import Job
from qcg.appscheduler.errors import IllegalResourceRequirements


def test_job_description_simple():

    # missing 'name' element
    with pytest.raises(Exception):
        jobd = '{ }'
        job = Job(**json.loads(jobd))

    # missing 'execution' element
    with pytest.raises(Exception):
        jobd = '{ "name": "job1" }'
        job = Job(**json.loads(jobd))

    # missing 'resources' element
    with pytest.raises(Exception):
        jobd = '{ "name": "job1", "execution": { "exec": "/bin/date" } }'
        job = Job(**json.loads(jobd))

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2  } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with minimal resource requirements"


def test_job_description_resources():
    # number of cores as a number
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2  } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with integer number of cores"

    # number of cores as an exact object
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "exact": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as an exact object"

    # number of cores as a range object
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2, "max": 3 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as a range object"

    # number of cores as a range object, with one of the boundary
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as a range object with only min boundary"

    # number of cores as a range object, with one of the boundary
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "max": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as a range object with only max boundary"

    # exact number with range
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "exact": 2, "min": 1, "max": 3 } } }"""
        job = Job(**json.loads(jobd))

    # exact number with one of the range boundary
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "exact": 2, "min": 1 } } }"""
        job = Job(**json.loads(jobd))

    # number of cores negative
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": -2 } }"""
        job = Job(**json.loads(jobd))

    # 'max' greater than 'min' in range object
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "min": 4, "max": 3 } } }"""
        job = Job(**json.loads(jobd))

    # range boundary negative
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "min": -2 } } }"""
        job = Job(**json.loads(jobd))

    # range boundary negative
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "min": -4, "max": -1 } } }"""
        job = Job(**json.loads(jobd))

    # crs without cores count
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "nodeCrs": { "gpu": 1 } } }"""
        job = Job(**json.loads(jobd))

    # gpu cr
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "gpu": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (gpu)"

    # mem cr
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "mem": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (mem)"

    # gpu cr with non-uniform letter case
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "GpU": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (gpu)"

    # mem cr with non-uniform letter case
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "meM": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (mem)"

    # many crs with non-uniform letter case
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "meM": 1, "gPU": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (mem)"

    # unknown cr
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "strange_cr": 1 } } }"""
        job = Job(**json.loads(jobd))

    # gpu cr without integer value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": "1" } } }"""
        job = Job(**json.loads(jobd))

    # mem cr without integer value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "mem": "one" } } }"""
        job = Job(**json.loads(jobd))

    # gpu cr with negative integer value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": -1 } } }"""
        job = Job(**json.loads(jobd))

    # gpu cr with 0
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": 0 } } }"""
        job = Job(**json.loads(jobd))

    # repeating gpu cr
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": 1, "GpU": 2 } } }"""
        job = Job(**json.loads(jobd))

    # repeating gpu cr
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": 1, "mem": 2, "GpU": 2 } } }"""
        job = Job(**json.loads(jobd))


def test_job_description_serialization():
    # gpu cr
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "gpu": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (gpu)"
    job_dict = job.toDict()

    job_clone = json.loads(job.toJSON())
    assert job_dict == job_clone
