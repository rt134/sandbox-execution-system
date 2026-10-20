import os

# Set development environment for automated test runs unless explicitly set
if "ENV" not in os.environ and "ENVIRONMENT" not in os.environ and "SANDBOX_ENVIRONMENT" not in os.environ:
    os.environ["ENV"] = "development"
    os.environ["SANDBOX_ENVIRONMENT"] = "development"
