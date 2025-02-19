import atoti as tt
import os

def start_application(session):
    movies_table = session.read_csv(
        "s3://atoti-paas-resource-cloudintegration-65867d56-8ba8-4c18-826f-8e/movie_data/movies_cleaned.csv",
        keys = {"id", "year", "month"},
        table_name = "Movies"
    )
    genre_table = session.read_csv(
        "s3://atoti-paas-resource-cloudintegration-65867d56-8ba8-4c18-826f-8e/movie_data/genres_cleaned.csv",
        keys={"index"},
        table_name="Genres"
    )
    cast_table = session.read_csv(
        "s3://atoti-paas-resource-cloudintegration-65867d56-8ba8-4c18-826f-8e/movie_data/cast_cleaned.csv",
        keys={"index"},
        table_name="Cast"
    )
    movies_table.join(genre_table, movies_table["id"] == genre_table["id"])
    movies_table.join(cast_table, movies_table["id"] == cast_table["movie_id"])
    cube = session.create_cube(movies_table)
    h, l, m = cube.hierarchies, cube.levels, cube.measures
    m["profit"] = m["revenue.SUM"] - m["budget.SUM"]
    m["profit rate"] = m["profit"] / m["budget.SUM"]
    m["Max profit"] = tt.agg.max(
    m["profit"],
        scope = tt.OriginScope({l["id"]})
    )
    m["Max profit movie"] = tt.agg.max_member(
        m["profit"],
        l["title"]
    )
    m["cumulative revenue"] = tt.agg.sum(
        m["revenue.SUM"],
        scope=tt.CumulativeScope(l["year"], dense = True),
    )
    m["Total box office"] = tt.agg.sum(
        m["revenue.SUM"],
        scope = tt.OriginScope({l["actor_name"]}),
    )
    genre_simulation = cube.create_parameter_simulation(
        "Year Simulation",
        levels = [l["year"]],
        measures={"year parameter": 1.0}
    )
    genre_simulation += ("Epidemic", 2012, 0.3)
    genre_simulation += ("Resumption", 2013, 1.2)
    m["revenue.SUM"] = tt.agg.sum(
        tt.agg.sum(movies_table["revenue"]) * m["year parameter"],
        scope=tt.OriginScope({l["year"]}),
    )


def main():
    with tt.Session._connect("127.0.0.1") as session:
        start_application(session)


def local_main():
    with tt.Session.start() as session:
        start_application(session)


if __name__ == "__main__":
    main()

