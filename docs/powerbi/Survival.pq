let
    BaseUrl = "http://localhost:5000",
    Response = Json.Document(
        Web.Contents(
            BaseUrl,
            [RelativePath = "survival/Restaurants"]
        )
    ),
    Points = Response[points],
    Rows =
        if List.IsEmpty(Points) then
            #table({"time_days", "at_risk", "events", "survival_probability"}, {})
        else
            Table.FromRecords(Points)
in
    Rows
