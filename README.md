# Twitch Big Data Project

## Setup Guide

### 1. Install NodeJS

Download and install [NodeJS](https://nodejs.org/en/download/)

### 2. Clone the repository

Clone the repo with **SSH** or **HTTPS**:

  - SSH: `git clone git@github.com:Purple-Team-SIC/twitch-bigdata-ML-project.git`
  - HTTPS: `git clone https://github.com/Purple-Team-SIC/twitch-bigdata-ML-project.git`

### 3. Install required npm modules

Change directory to the repository folder:

`cd twitch-big-data-project`

Install npm required modules:

`npm install`

### 4. Run the twitch chat "reader"!

`node app.js`

### 5. Run the Flume agent!

`flume-ng agent --name agent1 --conf flume.conf`

### 6. Finnaly, run the real-time message analysis!

Open **analysis.ipynb** in Jupyter Notebook

Execute each line 1 by 1.

### 7. Enjoy!
