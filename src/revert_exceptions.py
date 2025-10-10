class JuiceNotEnough(Exception):
    pass

class RejectJob(Exception):
    pass

class PreflightError(Exception):
    """Errors that occur during simulation which are deterministic and non-recoverable."""
    pass

class ExecutionError(Exception):
    """Errors that occur during contract execution."""
    pass