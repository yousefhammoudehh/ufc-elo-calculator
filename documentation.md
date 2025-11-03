# UFC ELO Calculator API Documentation

Comprehensive reference for every publicly exposed endpoint in the UFC ELO Calculator service. All routes return a JSON envelope of the form:

```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { },
  "errors": null
}
```

Errors use the same envelope with a non-200 `status_code` and an `errors` collection explaining what went wrong.

Unless noted, the examples below assume the API is available at `http://localhost` and no authentication is required.

---

## 1. Analytics Endpoints (`/analytics`)

> Responses below are abbreviated; additional fields may be present in real data.

### 1.1 Top Current ELO — `GET /analytics/top-elo`
- **Query**: `limit` (1–100, default 20)
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-elo?limit=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      {
        "fighter_id": "ufc-f-123",
        "name": "Alex Example",
        "current_elo": 1865.4,
        "peak_elo": 1920.6,
        "entry_elo": 1500.0
      }
    ]
  }
  ```

### 1.2 Top Peak ELO — `GET /analytics/top-peak-elo`
- **Query**: `limit`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-peak-elo?limit=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_id": "ufc-f-888", "name": "Prime Legend", "current_elo": 1810.5, "peak_elo": 1954.2 }
    ]
  }
  ```

### 1.3 Fighter ELO History — `GET /analytics/fighter-elo/{fighter_id}`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/fighter-elo/ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "fighter": { "fighter_id": "ufc-f-123", "name": "Alex Example", "current_elo": 1820.5 },
      "points": [
        {
          "bout_id": "bout-001",
          "event_date": "2023-02-12",
          "elo_before": 1501.2,
          "elo_after": 1533.6,
          "delta": 32.4,
          "opponent_name": "Chris Opp",
          "result": "W"
        }
      ]
    }
  }
  ```

### 1.4 Top ELO Gains — `GET /analytics/top-elo-gains`
- **Query**: `limit`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-elo-gains?limit=2"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      {
        "fighter_name": "Clutch Finisher",
        "delta": 48.7,
        "opponent_name": "Former Champ",
        "event_name": "UFC 300",
        "event_date": "2024-04-13"
      }
    ]
  }
  ```

### 1.5 Lowest Positive ELO Gains — `GET /analytics/lowest-elo-gains`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/lowest-elo-gains?limit=2"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_name": "Split Decision", "delta": 0.4, "event_name": "Fight Night 210" }
    ]
  }
  ```

### 1.6 Top ELO Losses — `GET /analytics/top-elo-losses`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-elo-losses?limit=2"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_name": "Upset Victim", "delta": -52.1, "opponent_name": "Unknown Prospect" }
    ]
  }
  ```

### 1.7 ELO Movers — `GET /analytics/elo-movers`
- **Query**: `direction`, `window_days` or `range`, `limit`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/elo-movers?direction=gains&range=60d&limit=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_id": "ufc-f-900", "fighter_name": "Hot Streak", "delta": 76.5, "fights": 3, "last_event_date": "2024-05-04" }
    ]
  }
  ```

### 1.8 Top Net Career Gain — `GET /analytics/top-elo-gain`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-elo-gain?limit=2"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_id": "ufc-f-777", "name": "Career Climber", "delta": 310.4 }
    ]
  }
  ```

### 1.9 Top Peak ELO Gain — `GET /analytics/top-peak-elo-gain`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-peak-elo-gain?limit=2"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_id": "ufc-f-778", "name": "Peak Riser", "delta": 285.1 }
    ]
  }
  ```

### 1.10 Random Bouts — `GET /analytics/random-bouts`
- **Query**: `limit`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/random-bouts?limit=2"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      {
        "event_name": "UFC 199",
        "event_date": "2016-06-04",
        "fighters": [
          { "fighter_name": "Michael Bisping", "delta": 42.3, "outcome": "W" },
          { "fighter_name": "Luke Rockhold", "delta": -42.3, "outcome": "L" }
        ]
      }
    ]
  }
  ```

### 1.11 Rankings History — `GET /analytics/rankings-history`
- **Query**: `start_year`, `end_year`, `top`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/rankings-history?start_year=2020&end_year=2021&top=5"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      {
        "label": "2020",
        "entries": [ { "rank": 1, "fighter_id": "ufc-f-100", "name": "Champion", "elo": 1884.2 } ]
      }
    ]
  }
  ```

### 1.12 Yearly ELO Gains — `GET /analytics/yearly-elo-gains`
- **Query**: `year`, `limit`, `offset`, `page_size`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/yearly-elo-gains?year=2023&limit=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "rank": 1, "fighter_id": "ufc-f-300", "fighter_name": "Breakout Name", "delta": 92.4, "fights": 4 }
    ]
  }
  ```

### 1.13 Ranking Years — `GET /analytics/ranking-years`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/ranking-years"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [1993, 1994, 1995, 2023, 2024]
  }
  ```

### 1.14 Rankings Year — `GET /analytics/rankings-year`
- **Query**: `year`, optional `top`, `offset`, `page_size`, `division`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/rankings-year?year=2024&top=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "label": "2024",
      "date": "2024-12-31",
      "entries": [ { "rank": 1, "fighter_id": "ufc-f-123", "name": "Alex Example", "elo": 1890.4 } ]
    }
  }
  ```

### 1.15 Head-to-Head Probability — `GET /analytics/h2h`
- **Query**: `fighter1`, `fighter2`, `mode1`, `mode2`, `year1`, `year2`, `adjust`, `ewma_hl`, `five_round`, `title`, `explain`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/h2h?fighter1=ufc-f-123&fighter2=ufc-f-456&adjust=best&explain=true"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "fighter1_id": "ufc-f-123",
      "fighter2_id": "ufc-f-456",
      "fighter1_name": "Alex Example",
      "fighter2_name": "Jordan Sample",
      "P1": 0.62,
      "P2": 0.38,
      "odds1": { "decimal": 1.61, "american": -163 },
      "winner_pred": "Alex Example by decision"
    }
  }
  ```

### 1.16 Hazard Histogram — `GET /analytics/hazard`
- **Query**: `fighter_id`, `five_round`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/hazard?fighter_id=ufc-f-123&five_round=false"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "bins": [ { "window": "0-5", "finishes_for": 3, "finishes_against": 0 } ],
      "durability": 0.84
    }
  }
  ```

### 1.17 Division Roster — `GET /analytics/division`
- **Query**: `code`, `top`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/division?code=104&top=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "division_code": 104,
      "division_name": "Men's Lightweight (155)",
      "active_count": 42,
      "rows": [
        {
          "fighter_id": "ufc-f-555",
          "fighter_name": "Lightweight Leader",
          "elo": 1875.1,
          "elo_progress": 130.6,
          "recent_fights": 3,
          "last_event_date": "2024-06-01"
        }
      ]
    }
  }
  ```

### 1.18 Form Leaders — `GET /analytics/form-top`
- **Query**: `window`, `n`, `half_life_days`, `top`, `min_recent_fights`, `recent_days`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/form-top?window=fights&n=5&top=3&min_recent_fights=2&recent_days=730"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      {
        "fighter_id": "ufc-f-600",
        "fighter_name": "In-Form Contender",
        "fi": 0.283,
        "recent_fights": 4,
        "last_event_date": "2024-05-18"
      }
    ]
  }
  ```

### 1.19 Fighter Career Stats — `GET /analytics/fighter-career-stats/{fighter_id}`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/fighter-career-stats/ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "total_fights": 28,
      "wins": 23,
      "losses": 5,
      "streak": 6,
      "title_defenses": 1
    }
  }
  ```

### 1.20 Latest Event ELO — `GET /analytics/latest-event-elo`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/latest-event-elo"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "event_id": "event-400",
      "event_name": "UFC 305",
      "event_date": "2024-08-17",
      "shock_index": 1.24,
      "title_bouts": 2
    }
  }
  ```

### 1.21 Event ELO — `GET /analytics/event-elo`
- **Query**: `event_id`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/event-elo?event_id=event-305"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "event_id": "event-305",
      "event_name": "Fight Night 300",
      "event_date": "2024-05-10",
      "entries": [
        { "bout_id": "bout-900", "fighter1_delta": 18.2, "fighter2_delta": -18.2, "shock": 1.9 }
      ]
    }
  }
  ```

### 1.22 Events List — `GET /analytics/events`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/events"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "event_id": "event-305", "name": "Fight Night 300", "event_date": "2024-05-10" }
    ]
  }
  ```

### 1.23 Top Fighter Stats — `GET /analytics/top-stats`
- **Query**: `metric`, `limit`, `since_year`, `division`, `rate`, `adjusted`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/top-stats?metric=kd&limit=3&rate=per15&adjusted=true"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_id": "ufc-f-411", "fighter_name": "KO Artist", "value": 1.92, "fights": 5 }
    ]
  }
  ```

### 1.24 Plus/Minus — `GET /analytics/plusminus`
- **Query**: `fighter_id`, `metric`, `since_year`, `opp_window_months`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/plusminus?fighter_id=ufc-f-123&metric=sig_strikes&opp_window_months=18"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "metric": "sig_strikes",
      "fighter_rate": 7.1,
      "opponent_rate": 4.9,
      "plusminus": 2.2
    }
  }
  ```

### 1.25 Consistency & Versatility — `GET /analytics/consistency-versatility`
- **Query**: `fighter_id`, `k`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/consistency-versatility?fighter_id=ufc-f-123&k=6"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "sd_elo_delta": 12.4,
      "finish_distribution": { "KO": 0.45, "SUB": 0.25, "DEC": 0.30 },
      "versatility": 0.68
    }
  }
  ```

### 1.26 Divisions List — `GET /analytics/divisions`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/divisions"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "code": 101, "name": "Men's Flyweight (125)", "max_weight_lbs": 125 }
    ]
  }
  ```

### 1.27 Division Rankings — `GET /analytics/division-rankings`
- **Query**: `division`, `metric`, `year`, `active_only`, `limit`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/division-rankings?division=170&metric=current&limit=5"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "fighter_id": "ufc-f-700", "fighter_name": "Welterweight Ace", "elo": 1830.6, "rank": 1 }
    ]
  }
  ```

### 1.28 Division Parity — `GET /analytics/division-parity`
- **Query**: `division`, `year`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/division-parity?division=170&year=2024"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "division": 170,
      "year": 2024,
      "gini": 0.21,
      "std_dev": 42.7
    }
  }
  ```

### 1.29 Division Churn — `GET /analytics/division-churn`
- **Query**: `division`, `year`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/division-churn?division=170&year=2024"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "entries": ["ufc-f-770", "ufc-f-771"],
      "exits": ["ufc-f-640"],
      "retained": 7
    }
  }
  ```

### 1.30 Form Index — `GET /analytics/form`
- **Query**: `fighter_id`, `window`, `n`, `half_life_days`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/form?fighter_id=ufc-f-123&window=fights&n=5"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "fighter_id": "ufc-f-123",
      "fi": 0.214,
      "count": 5,
      "avg_opp_elo": 1765.3,
      "series": [ { "date": "2024-02-03", "residual": 0.18 } ]
    }
  }
  ```

### 1.31 Momentum — `GET /analytics/momentum`
- **Query**: `fighter_id`, `k`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/momentum?fighter_id=ufc-f-123&k=6"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "fighter_id": "ufc-f-123",
      "slope_per_fight": 6.2,
      "slope_per_180d": 18.7
    }
  }
  ```

### 1.32 Rates Per Minute — `GET /analytics/rates`
- **Query**: `fighter_id`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/rates?fighter_id=ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "sig_strikes_attempted_per_min": 12.3,
      "sig_strikes_landed_per_min": 6.7,
      "control_seconds_per_min": 32.1
    }
  }
  ```

### 1.33 Event Shock — `GET /analytics/event-shock`
- **Query**: `event_id` (optional; latest when omitted)
- **Example Request**
  ```bash
  curl "http://localhost/analytics/event-shock?event_id=event-300"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "event_id": "event-300",
      "shock_index": 1.92,
      "top_bouts": [ { "bout_id": "bout-1200", "shock": 2.8, "delta_sum": 62.3 } ]
    }
  }
  ```

### 1.34 Latest Event Shock — `GET /analytics/latest-event-shock`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/latest-event-shock"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "event_id": "event-305",
      "shock_index": 1.24
    }
  }
  ```

### 1.35 Events Shock Top — `GET /analytics/events-shock-top`
- **Query**: `limit`, `order`, `max_events`, `window_days`/`range`, `type`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/events-shock-top?type=shocking&range=90d&limit=3"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      { "event_id": "event-280", "event_name": "Fight Night 280", "shock_index": 2.14 }
    ]
  }
  ```

### 1.36 Strength of Schedule — `GET /analytics/sos`
- **Query**: `fighter_id`, `window`, `n`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/sos?fighter_id=ufc-f-123&window=days&n=365"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "fighter_id": "ufc-f-123",
      "mean": 1782.4,
      "median": 1770.0,
      "count": 4
    }
  }
  ```

### 1.37 Quality Wins — `GET /analytics/quality-wins`
- **Query**: `fighter_id`, `elo_threshold`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/quality-wins?fighter_id=ufc-f-123&elo_threshold=1850"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "wins": [
        { "opponent_id": "ufc-f-900", "event_name": "UFC 300", "method": "DEC", "opponent_elo": 1872.3 }
      ],
      "total": 2
    }
  }
  ```

### 1.38 Style Profile — `GET /analytics/style-profile`
- **Query**: `fighter_id`
- **Example Request**
  ```bash
  curl "http://localhost/analytics/style-profile?fighter_id=ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": {
      "distance_ss_pct": 0.62,
      "clinch_ss_pct": 0.12,
      "ground_ss_pct": 0.26,
      "finish_mix": { "KO": 0.35, "SUB": 0.25, "DEC": 0.40 }
    }
  }
  ```

---

## 2. Bout Participant Endpoints (`/bout-participants`)

### 2.1 List by Bout — `GET /bout-participants/by-bout/{bout_id}`
- **Example Request**
  ```bash
  curl "http://localhost/bout-participants/by-bout/ufc-bout-1001"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [
      {
        "bout_id": "ufc-bout-1001",
        "fighter_id": "ufc-f-123",
        "outcome": "W",
        "elo_delta": 27.4
      }
    ]
  }
  ```

### 2.2 List by Fighter — `GET /bout-participants/by-fighter/{fighter_id}`
- **Example Request**
  ```bash
  curl "http://localhost/bout-participants/by-fighter/ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": [ { "bout_id": "ufc-bout-1001", "outcome": "W" } ]
  }
  ```

### 2.3 Get Participation — `GET /bout-participants/{bout_id}/{fighter_id}`
- **Example Request**
  ```bash
  curl "http://localhost/bout-participants/ufc-bout-1001/ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": { "bout_id": "ufc-bout-1001", "fighter_id": "ufc-f-123", "outcome": "W" }
  }
  ```

### 2.4 Fighter Record — `GET /bout-participants/record/{fighter_id}`
- **Example Request**
  ```bash
  curl "http://localhost/bout-participants/record/ufc-f-123"
  ```
- **Example Response**
  ```json
  {
    "status_code": 200,
    "message": "Ok",
    "data": { "wins": 23, "losses": 5, "draws": 0, "no_contests": 0 }
  }
  ```

---

## 3. Bout Endpoints (`/bouts`)

### 3.1 List Bouts — `GET /bouts/`
```bash
curl "http://localhost/bouts/"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [
    {
      "bout_id": "ufc-bout-1001",
      "event_id": "event-305",
      "weight_class_code": 104
    }
  ]
}
```

### 3.2 Get Bout — `GET /bouts/{bout_id}`
```bash
curl "http://localhost/bouts/ufc-bout-1001"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "bout_id": "ufc-bout-1001", "event_id": "event-305" }
}
```

### 3.3 Create Bout — `POST /bouts/`
```bash
curl -X POST "http://localhost/bouts/" \
  -H "Content-Type: application/json" \
  -d '{
        "bout_id": "test-001",
        "event_id": "event-123",
        "weight_class_code": 104,
        "rounds_scheduled": 3,
        "is_title_fight": false
      }'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "bout_id": "test-001", "event_id": "event-123" }
}
```

### 3.4 Bout Details — `GET /bouts/{bout_id}/details`
```bash
curl "http://localhost/bouts/ufc-bout-1001/details"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": {
    "bout_id": "ufc-bout-1001",
    "fighters": [
      { "fighter_id": "ufc-f-123", "elo_delta": 27.4 },
      { "fighter_id": "ufc-f-456", "elo_delta": -27.4 }
    ]
  }
}
```

---

## 4. Event Endpoints (`/events`)

### 4.1 List Events — `GET /events/`
```bash
curl "http://localhost/events/?page=1&limit=2"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [
    { "event_id": "event-305", "name": "Fight Night 300", "event_date": "2024-05-10" }
  ]
}
```

### 4.2 Create Event — `POST /events/`
```bash
curl -X POST "http://localhost/events/" \
  -H "Content-Type: application/json" \
  -d '{
        "event_id": "event-999",
        "name": "Test Event",
        "event_date": "2024-12-01",
        "location": "Las Vegas, NV"
      }'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "event_id": "event-999", "name": "Test Event" }
}
```

### 4.3 Get Event by Link — `GET /events/by-link`
```bash
curl "http://localhost/events/by-link?event_link=https://ufcstats.com/event-details/test"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "event_id": "event-305", "event_link": "https://ufcstats.com/event-details/test" }
}
```

### 4.4 Get Event — `GET /events/{event_id}`
```bash
curl "http://localhost/events/event-305"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "event_id": "event-305", "name": "Fight Night 300" }
}
```

---

## 5. Fighter Endpoints (`/fighters`)

### 5.1 List Fighters — `GET /fighters/`
```bash
curl "http://localhost/fighters/"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [ { "fighter_id": "ufc-f-123", "name": "Alex Example" } ]
}
```

### 5.2 Create Fighter — `POST /fighters/`
```bash
curl -X POST "http://localhost/fighters/" \
  -H "Content-Type: application/json" \
  -d '{
        "fighter_id": "ufc-f-999",
        "name": "New Prospect",
        "country": "USA"
      }'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "fighter_id": "ufc-f-999", "name": "New Prospect" }
}
```

### 5.3 Get Fighter by Stats Link — `GET /fighters/by-stats-link`
```bash
curl "http://localhost/fighters/by-stats-link?stats_link=https://ufcstats.com/fighter-details/example"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "fighter_id": "ufc-f-123", "stats_link": "https://ufcstats.com/fighter-details/example" }
}
```

### 5.4 Get Fighter by Tapology Link — `GET /fighters/by-tapology-link`
```bash
curl "http://localhost/fighters/by-tapology-link?tapology_link=https://www.tapology.com/fightcenter/fighters/example"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "fighter_id": "ufc-f-123", "tapology_link": "https://www.tapology.com/fightcenter/fighters/example" }
}
```

### 5.5 Search Fighters — `GET /fighters/search`
```bash
curl "http://localhost/fighters/search?q=Silva&limit=5"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [ { "fighter_id": "ufc-f-200", "name": "Anderson Silva" } ]
}
```

### 5.6 Search Fighters (Paginated) — `GET /fighters/search-paginated`
```bash
curl "http://localhost/fighters/search-paginated?q=Jones&page=1&limit=2&sort_by=current_elo&order=desc"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": {
    "items": [ { "fighter_id": "ufc-f-001", "name": "Jon Jones" } ],
    "total": 1,
    "page": 1,
    "limit": 2,
    "sort_by": "current_elo",
    "order": "desc"
  }
}
```

### 5.7 Get Fighter — `GET /fighters/{fighter_id}`
```bash
curl "http://localhost/fighters/ufc-f-123"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "fighter_id": "ufc-f-123", "name": "Alex Example" }
}
```

---

## 6. Pre-UFC Bout Endpoints (`/pre-ufc-bouts`)

### 6.1 By Fighter — `GET /pre-ufc-bouts/by-fighter/{fighter_id}`
```bash
curl "http://localhost/pre-ufc-bouts/by-fighter/ufc-f-123"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [ { "promotion": "Regional MMA", "result": "W" } ]
}
```

### 6.2 By Promotion — `GET /pre-ufc-bouts/by-promotion/{promotion_id}`
```bash
curl "http://localhost/pre-ufc-bouts/by-promotion/8b61da4a-54d9-4a77-9a2a-111111111111"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [ { "fighter_id": "ufc-f-123", "result": "W" } ]
}
```

### 6.3 By Fighter & Promotion — `GET /pre-ufc-bouts/by-fighter-and-promotion/{fighter_id}/{promotion_id}`
```bash
curl "http://localhost/pre-ufc-bouts/by-fighter-and-promotion/ufc-f-123/8b61da4a-54d9-4a77-9a2a-111111111111"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [ { "result": "W" } ]
}
```

### 6.4 Record — `GET /pre-ufc-bouts/record/{fighter_id}`
```bash
curl "http://localhost/pre-ufc-bouts/record/ufc-f-123"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "wins": 10, "losses": 1, "draws": 0 }
}
```

---

## 7. Promotion Endpoints (`/promotions`)

### 7.1 List Promotions — `GET /promotions/`
```bash
curl "http://localhost/promotions/?page=1&limit=2"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": [ { "promotion_id": "promo-001", "name": "UFC" } ]
}
```

### 7.2 Create Promotion — `POST /promotions/`
```bash
curl -X POST "http://localhost/promotions/" \
  -H "Content-Type: application/json" \
  -d '{
        "promotion_id": "promo-999",
        "name": "Regional MMA",
        "strength": 0.4
      }'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "promotion_id": "promo-999", "name": "Regional MMA" }
}
```

### 7.3 Get Promotion by Link — `GET /promotions/by-link`
```bash
curl "http://localhost/promotions/by-link?promotion_link=https://ufcstats.com/promotion-details/test"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "promotion_id": "promo-001", "promotion_link": "https://ufcstats.com/promotion-details/test" }
}
```

---

## 8. Ingestion Endpoints (`/ingest`)

### 8.1 Seed Events by ID — `POST /ingest/events`
```bash
curl -X POST "http://localhost/ingest/events" \
  -H "Content-Type: application/json" \
  -d '{"event_ids": ["ufc-285", "ufc-286"]}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": {
    "events_seeded_count": 2,
    "fighters_seeded_count": 40
  }
}
```

### 8.2 Seed Event ELO — `POST /ingest/events-elo`
```bash
curl -X POST "http://localhost/ingest/events-elo" \
  -H "Content-Type: application/json" \
  -d '{"event_ids": ["ufc-285"]}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "items": [ { "event_id": "ufc-285", "status": "seeded" } ] }
}
```

### 8.3 Seed Events by Link — `POST /ingest/events-by-link`
```bash
curl -X POST "http://localhost/ingest/events-by-link" \
  -H "Content-Type: application/json" \
  -d '{"event_links": ["https://ufcstats.com/event-details/abc"]}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "events_seeded_count": 1 }
}
```

### 8.4 Scrape Event Fights & Seed ELO — `POST /ingest/events-fights-elo`
```bash
curl -X POST "http://localhost/ingest/events-fights-elo" \
  -H "Content-Type: application/json" \
  -d '{"event_ids": ["ufc-285"]}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "items": [ { "event_id": "ufc-285", "fights_seeded": 12 } ] }
}
```

### 8.5 Seed First Unseeded Event Fights — `POST /ingest/fights-first`
```bash
curl -X POST "http://localhost/ingest/fights-first" \
  -H "Content-Type: application/json" \
  -d '{"limit": 1}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "items": [ { "event_id": "ufc-100", "fights_seeded": 5 } ] }
}
```

### 8.6 Seed First Unseeded Events — `POST /ingest/events-first`
```bash
curl -X POST "http://localhost/ingest/events-first" \
  -H "Content-Type: application/json" \
  -d '{"limit": 2}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": {
    "events_seeded_count": 2,
    "first_event": { "event_id": "ufc-10" },
    "last_event": { "event_id": "ufc-11" }
  }
}
```

### 8.7 Seed Fighters by Link — `POST /ingest/fighters-by-link`
```bash
curl -X POST "http://localhost/ingest/fighters-by-link" \
  -H "Content-Type: application/json" \
  -d '{"fighter_links": ["https://www.tapology.com/fightcenter/fighters/example"]}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "fighters_seeded_count": 1 }
}
```

### 8.8 Seed Single Fighter by Link — `POST /ingest/fighter-by-link`
```bash
curl -X POST "http://localhost/ingest/fighter-by-link" \
  -H "Content-Type: application/json" \
  -d '{"tapology_link": "https://www.tapology.com/fightcenter/fighters/example", "stats_link": null}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "fighter_id": "ufc-f-123", "name": "Alex Example" }
}
```

---

## 9. Maintenance Endpoints (`/maintenance`)

### 9.1 Flush Cache — `POST /maintenance/cache/flush`
```bash
curl -X POST "http://localhost/maintenance/cache/flush"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "prefix_results": [], "total_removed": 0 }
}
```

### 9.2 Invalidate Cache Prefixes — `POST /maintenance/cache/invalidate`
```bash
curl -X POST "http://localhost/maintenance/cache/invalidate" \
  -H "Content-Type: application/json" \
  -d '{"prefixes": ["analytics:rank"], "batch_size": 500}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": {
    "prefix_results": [ { "prefix": "analytics:rank", "keys_removed": 212 } ],
    "total_removed": 212
  }
}
```

### 9.3 Reseed Entry ELO — `POST /maintenance/elo/reseed`
```bash
curl -X POST "http://localhost/maintenance/elo/reseed" \
  -H "Content-Type: application/json" \
  -d '{"default_strength": 0.35, "dry_run": true}'
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": {
    "processed_fighters": 680,
    "updated": 0,
    "dry_run": true
  }
}
```

### 9.4 Sync Fighters — `POST /maintenance/sync-fighters`
```bash
curl -X POST "http://localhost/maintenance/sync-fighters?throttle_ms=100"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "updated": 12, "skipped": 3 }
}
```

### 9.5 Seed Event Names — `POST /maintenance/seed-event-names`
```bash
curl -X POST "http://localhost/maintenance/seed-event-names?throttle_ms=100"
```
```json
{
  "status_code": 200,
  "message": "Ok",
  "data": { "updated": 45, "skipped": 5 }
}
```

---

### Final Notes
- Respect query parameter constraints to avoid HTTP 422 validation errors.
- Long-running ingestion or reseed operations should be monitored via server logs.
- When integrating, always check for non-200 `status_code` responses and surface `errors` to callers.

