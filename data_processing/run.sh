# basic script to load up any necessary environment variables
export $(grep -v '^#' .env | xargs) && python ./run.py