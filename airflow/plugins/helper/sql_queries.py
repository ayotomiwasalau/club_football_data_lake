class HelperQueries():
    
    create_match_event_tbl = ("""
    CREATE TABLE "match_event" (
      "id" int,
      "club_id" int REFERENCES "club" ("id"),
      "match_id" int REFERENCES "match" ("id"),
      "players_id" int REFERENCES "player" ("id"),
      "matchPeriod" varchar,
      "eventSec" float,
      "eventName" varchar,
      "action" varchar,
      "modifier" varchar,
      "x_begin" int,
      "y_begin" int,
      "x_end" int,
      "y_end" int,
      "is_success" boolean,
      PRIMARY KEY ("id")
    );
    """)
    
    create_match_tbl = ("""
    CREATE TABLE "match" (
      "id" int,
      "dateutc" varchar,
      "competition" varchar,
      "season" varchar,
      "venue" varchar,
      "home_club" varchar,
      "away_club" varchar,
      "winner" varchar,
      "goal_by_home_club" varchar,
      "goal_by_away_club" varchar,
      "referee_id" int REFERENCES "referee" ("id"),
      PRIMARY KEY ("id")
    );
    """)

    create_club_tbl = ("""
    CREATE TABLE "club" (
      "id" int,
      "name" varchar,
      "officialName" varchar,
      "country" varchar,
      PRIMARY KEY ("id")
    );
    """)

    create_player_tbl = ("""
    CREATE TABLE "player" (
      "id" int,
      "firstName" varchar,
      "lastName" varchar,
      "birthDate" varchar,
      "country" varchar,
      "position" varchar,
      "foot" varchar,
      "height" int,
      PRIMARY KEY ("id")
    );
    """)


    create_referee_tbl = ("""
    CREATE TABLE "referee" (
      "id" int,
      "firstName" varchar,
      "lastName" varchar,
      "birthDate" varchar,
      "country" varchar,
      PRIMARY KEY ("id")
    );
    """)

    
#     COPY_SQL = """
#     COPY {}
#     FROM '{}'
#     ACCESS_KEY_ID '{{}}'
#     SECRET_ACCESS_KEY '{{}}'
#     IGNOREHEADER 1
#     DELIMITER ','
#     """

#     match_event_load_SQL = COPY_SQL.format(
#         "match_event",
#         "s3://udacity-dend/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
#     )

#     match_load_SQL = COPY_SQL.format(
#         "match",
#         "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
#     )

#     club_load_SQL = COPY_SQL.format(
#         "club",
#         "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
#     )
    
#     player_load_SQL = COPY_SQL.format(
#         "player",
#         "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
#     )
    
#     manager_load_SQL = COPY_SQL.format(
#         "manager",
#         "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
#     )

#     create_manager_tbl = ("""                        
#     CREATE TABLE "manager" (
#       "id" int,
#       "first_name" varchar,
#       "last_name" varchar,
#       "country" varchar,
#       PRIMARY KEY ("id")
#     );
#     """)

    
#     referee_load_SQL = COPY_SQL.format(
#         "referee",
#         "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
#     )

    






