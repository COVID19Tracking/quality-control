[![COVID19Tracking](https://circleci.com/gh/COVID19Tracking/quality-control.svg?style=svg)](https://circleci.com/gh/COVID19Tracking/quality-control)


# QUALITY CONTROL

Slack channel: #data-entry-qa

Consistency checks for [CovidTracking](covidtracking.com) data 

## Purpose

To verify internal and external consistency of reported COVID counts tracked by the team.

This repo contains automated checks against the [working spreadsheet](https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528) and against the published [api](https://covidtracking.com/api).


## Setup

1. Set environment

   	install python3.8.1 or later
	python3 -m venv qc-env
	source qc-env/bin/activate

2. Install requirements 

        pip install -r requirements.txt

## Running 

You can either run this repo as a client or as a Flask app that sends requests to a Pyro4 RPC server.

#### Command Line

        python run_quality_cli.py [-w, --working] [-d, --daily] [-x, --history]

#### Web Server

1. Install requirements 

        pip install -r requirements.txt

2. Open a terminal and run

        python flaskapp.py

3. Open a separate terminal and run

        python run_quality_service.py

4. Open http://localhost:5000 and you should see the client running 

   <img src="https://raw.githubusercontent.com/COVID19Tracking/quality-control/master/static/images/github/index.png" width="500">
   <br></br>
   <img src="https://raw.githubusercontent.com/COVID19Tracking/quality-control/master/static/images/github/results_page.png" width="500">

# Approach

A user chooses to check either the [working dev Google sheet](https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528), [current api](https://covidtracking.com/api), or [history api](https://covidtracking.com/api). Each state's data (such as positives, deaths, negatives, totals,  pending tests, and others coming soon) are run independently against a series of checks. `./app/check_dataset.py` contains the list of applicable checks for each dataset and controls data and object passing to the `./app/checks.py` file, which implements the checking logic. 

Quality check results are served via a [Pyro](https://pyro4.readthedocs.io/en/stable/) proxy that can send requests to the `./app/check_dataset` for checking execution. We then sit a Flask app on the front end so that users can interact wtih the proxy.

# Status / Implementation Notes

A list of current checks along with implemention assumptions / judgement calls is maintained in a spreadsheet in this repo ([`./resources/Quality\ Control\ Checks.xlsx`](https://github.com/COVID19Tracking/quality-control/resources))

**Current status:**

1. The team is working on deploying the app to a public facing node
2. There is still work to be done adding checks to the service. See the #data-entry-qa channel to get up to speed and check the COVID19Tracking [issues page](https://github.com/COVID19Tracking/issues) for open issues.

