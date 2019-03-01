from .job import job_types

# Making job classes available in module import
for _cls in job_types.values():
    globals()[_cls.__name__] = _cls
