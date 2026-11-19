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
                    window_days = "365",
                    include_all = "true"
                ]
            ]
        )
    ),
    Rows = Table.FromRecords(Response[results]),
    WithNegativeFraction =
        if Table.IsEmpty(Rows) then
            #table({"window_end", "review_count", "negative_count", "negative_fraction"}, {})
        else
            let
                Grouped = Table.Group(
                    Rows,
                    {"window_end"},
                    {
                        {"review_count", each List.Sum([review_count]), Int64.Type},
                        {"negative_count", each List.Sum([negative_count]), Int64.Type}
                    }
                ),
                WithFraction = Table.AddColumn(
                    Grouped,
                    "negative_fraction",
                    each if [review_count] = 0 then null else [negative_count] / [review_count],
                    type number
                )
            in
                WithFraction
in
    WithNegativeFraction
