let
    BaseUrl = "http://localhost:5000",
    Response = Json.Document(
        Web.Contents(
            BaseUrl,
            [
                RelativePath = "live/hotspots",
                Query = [
                    lon = "-75.16",
                    lat = "39.95",
                    radius_m = "5000",
                    window_days = "30"
                ]
            ]
        )
    ),
    Rows = Table.FromRecords(Response[results]),
    Result =
        if Table.IsEmpty(Rows) then
            #table({"window_end", "geohash", "review_count", "negative_count", "negative_fraction", "hotspot", "longitude", "latitude"}, {})
        else
            let
                Location = Table.ExpandRecordColumn(Rows, "location", {"coordinates"}, {"coordinates"}),
                Longitude = Table.AddColumn(Location, "longitude", each [coordinates]{0}, type number),
                Latitude = Table.AddColumn(Longitude, "latitude", each [coordinates]{1}, type number)
            in
                Latitude
in
    Result
