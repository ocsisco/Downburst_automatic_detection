# Downburst Automatic Detection

Automatic detection of downbursts from surface weather station data provided by
[AVAMET](https://www.avamet.org/) (the Valencian Association of Meteorology).

The pipeline runs in a loop: it downloads the latest station observations,
searches for the combination of meteorological signatures that characterise a
downburst, builds a dataset of detected events and publishes it to the
[downburst.csic.es](https://downburst.csic.es/) reporting platform.

## How detection works

For every station, the algorithm looks for an anomalous change in several
variables within a rolling time window, comparing each observation against the
mean of the preceding interval:

| Variable | Meaning | Default threshold | Window |
| --- | --- | --- | --- |
| `vent vel_max` | Maximum wind gust | +30 | 60 min |
| `vent vel_mit` | Mean wind speed | +20 | 60 min |
| `hrel mit_mit` | Mean relative humidity | ±5 | 30 min |
| `temp mit_mit` | Mean temperature | ±0 (any change) | 30 min |
| `prec tot_sum` | Accumulated precipitation | +0 | 60 min |

Additionally, each station must register a gust above `min_ws` (70 by default).

A downburst is reported when all of these signatures overlap in time at the
same station. The event is reported at the timestamp of the maximum gust, so
only one record per station is kept. All thresholds and windows are
configurable — see [Configuration](#configuration).

## Requirements

- Python 3.9+
- The packages listed in `requirements.txt`:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install paramiko  # required for the SFTP upload
```

## Configuration

Runtime options live in `config.yml`:

```yaml
# General settings
date: null          # "YYYY-MM-DD" to analyse a specific date, null for live mode
sleep_time: 30      # Seconds between pipeline runs
size_of_dataset: 8  # Hours of data to keep when running in live mode

# Detection settings
detection:
  min_ws: 70
  vent_vel_max:
    threshold: 30
    time_interval: "60min"
    only_increase: true
  # ... one block per variable (threshold, time_interval, only_increase)
```

Set `date` to a fixed day to reproduce a historical event; leave it as `null`
to process the last 24 hours and keep only the most recent
`size_of_dataset` hours.

Credentials are read from environment variables, typically via a `.env` file
(already git-ignored):

```dotenv
AVAMET_PASSWORD=your_avamet_token
FTP_HOST=sftp.example.org
FTP_USER=username
FTP_PASS=password
```

- `AVAMET_PASSWORD` is the AVAMET access token used to download the daily
  station tables.
- `FTP_HOST`, `FTP_USER` and `FTP_PASS` are used to upload the results over
  SFTP (port 22) to the web server.

## Usage

Run the full pipeline:

```bash
python main.py
```

Each iteration executes the following steps, and failures in one step do not
stop the loop:

1. `functions/get_data.py` — downloads AVAMET observations into
   `data/dataset_AVAMET.csv`.
2. `functions/detect_downbursts.py` — detects candidate downbursts and writes
   `data/downbursts.csv`.
3. `functions/generate_output.py` — builds `data/downbursts.json`, adding a
   prefilled report URL for each event.
4. `functions/send_json_to_website.py` — uploads the results to the web server
   over SFTP.

Every module can also be executed on its own, for example:

```bash
python -m functions.detect_downbursts
```

## Output

`data/downbursts.json` contains one object per detected event:

```json
[
  {
    "date": "2024-06-08 23:35:00",
    "codi": "c04m055e07",
    "vent gra_mit": 225,
    "vent vel_mit": 56.3,
    "vent vel_max": 86.9,
    "temp mit_mit": 13.7,
    "hrel mit_mit": 81.0,
    "pres mit_mit": 1005.2,
    "prec tot_sum": 2.0,
    "prec tot": 0.0,
    "latitud": 40.203522,
    "longitud": -0.267557,
    "altitud": 1164.0,
    "nom descriptiu": "Xodos - la Lloma Bernat",
    "url": "https://downburst.csic.es/en/report-a-downburst/?date=..."
  }
]
```

## Project structure

```
.
├── main.py                        # Pipeline loop
├── config.yml                     # Thresholds and runtime settings
├── requirements.txt               # Python dependencies
├── data/
│   └── downbursts.json            # Detected events (generated)
└── functions/
    ├── get_data.py                # AVAMET download and normalisation
    ├── detect_downbursts.py       # Anomaly detection and event merging
    ├── generate_output.py         # JSON export with report URLs
    └── send_json_to_website.py    # SFTP upload
```

## License

Released under the [MIT License](LICENSE).
