# Occupancy Detection Logic

## Lighting Event

- Detect a sudden increase in illumination greater than 100 lx within 1 minute.

- If detected, mark the room as “occupied”.

## CO₂ Increase Event

- In a 5-minute sliding window (updated every 1 minute), check for CO₂ readings that are non-decreasing.

- If such a trend exists, mark the room as “occupied”.

## Timeout for Absence

- If neither event is detected for 1 hour, mark the room as “unoccupied”.

# Flink Dashboard 
http://150.65.181.175:8081