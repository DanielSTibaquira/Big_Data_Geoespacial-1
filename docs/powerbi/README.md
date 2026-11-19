# Power BI LifeMap

Use Power BI Desktop on the same network as the deployed API. For each `.pq` file in this folder:

1. Select **Get data → Blank query → Advanced Editor**.
2. Paste the query and change `BaseUrl` to the deployed HTTPS API URL.
3. Rename the query to `Hotspots`, `Alerts`, `Survival`, or `NegativeTrend`.
4. Configure a map from hotspot/alert `latitude` and `longitude`, a table from alerts, a line chart from survival `time_days`/`survival_probability`, and a line chart from `window_end`/`negative_fraction`.
5. Enable refresh for the live views and refresh during the demo.

`NegativeTrend.pq` requests all geohash windows within the selected radius and computes the weighted negative-review proportion for each window. These files supply Power Query data; the `.pbix` report and its publication must be created with Power BI Desktop.
