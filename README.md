# DAGWorks Examples
This repository holds example projects that one can see on the DAGWorks platform.

## DAGWorks Platform
Visit https://www.dagworks.io to sign up!

# Examples
Here are some Hamilton examples, that use the DAGWorks Driver to log information, that 
you can subsequently view on the DAGWorks Platform! 

Note: you will not have write access for these examples, but if you create your own project, 
and update the project_id, username, and API key you will be able to write to your own project.
*Better yet*: use the dagworks CLI to generate these for you! 

```bash
pip install dagworks-sdk
dagworks init \
      --api-key API_KEY_HERE \
      --username EMAIL_HERE \
      --project-id PROJECT_ID_FROM_DAGWORKS_PLATFORM \
      --template [hello_world|machine_learning|time_series_feature_engineering|data_processing] \
      --location LOCATION_TO_CREATE_PROJECT
```

## Hello World
Example project that shows the "hello world" project that Hamilton often uses.
It's very basic, with one python module that defines a Hamilton dataflow to compute a few pandas columns.

To see this example - navigate [here](https://app.dagworks.io/dashboard/project/2).


## Data Processing
Example data processing project. This is a contrived example doing some basic data processing,
and how one might structure it with Hamilton and then subsequently view it in the DAGWorks platform.

To see this example - navigate [here](https://app.dagworks.io/dashboard/project/32).

## Machine Learning
Example machine learning project using the Iris data set. Look at me for a basic ML pipeline 
set up with Hamilton.

To see this example - navigate [here](https://app.dagworks.io/dashboard/project/29).

## Time Series Feature Engineering
Example project showing time-series feature engineering. This is a basic set of transforms based on
[F33](https://www.f33.ai)'s [WaffleML repostiory](https://github.com/F33AI/waffleml-examples/tree/master/kaggle_store_item_demand_forecasting)
that uses Hamilton for feature engineering. 

To see this example - navigate [here](https://app.dagworks.io/dashboard/project/31).


