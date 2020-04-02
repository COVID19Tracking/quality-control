[![COVID19Tracking](https://circleci.com/gh/COVID19Tracking/quality-control.svg?style=svg)](https://circleci.com/gh/COVID19Tracking/quality-control)


# QUALITY CONTROL

Consistency Checks for Spreadsheet

## Purpose

This is a repo for automated checks to the spreadsheet.

It fetchs the current data and a few recent archives and checks
that the numbers are internally consistent.

## Running 

1. Set environment

   	install python3.8.1 or later
	python3 -m venv qc-env
	source qc-env/bin/activate

1. Install requirements 

        pip install -r requirements.txt

2. Run existing checks

        python run_quality_checks.py

# Approach

Each state is checked independently. First, we run through a list
of general checks.  Then we run a second time with any state
specific tests.  Each check pass consists of comparing the current
day to the previous day.

# Status / Implementation Notes

1. This is not currently in production -- so don't worry about breaking things.
2. I am setting up the initial loop now. - Josh

