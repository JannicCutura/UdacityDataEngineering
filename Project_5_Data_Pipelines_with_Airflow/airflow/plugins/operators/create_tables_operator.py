from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook
import logging

class CreateTableOperator(BaseOperator):
    ui_color = '#456140'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.task_id = kwargs["task_id"]

    def execute(self, context):
        # setup variables
        logging.info(f"Executing task_id: {self.task_id}")
        aws_hook= AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id="redshift")
            
       
        tables = ['staging_events','staging_songs', 'songplays']
        for table in tables:
            logging.info(f"Executing: DROP TABLE IF EXISTS {table}")
            redshift.run(f"DROP TABLE IF EXISTS {table}")
 
        # create statements
        create_statements = {'staging_events':"""CREATE TABLE public.staging_events (
                                                artist varchar(256),
                                                auth varchar(256),
                                                firstname varchar(256),
                                                gender varchar(256),
                                                iteminsession int4,
                                                lastname varchar(256),
                                                length numeric(18,0),
                                                "level" varchar(256),
                                                location varchar(256),
                                                "method" varchar(256),
                                                page varchar(256),
                                                registration numeric(18,0),
                                                sessionid int4,
                                                song varchar(256),
                                                status int4,
                                                ts int8,
                                                useragent varchar(256),
                                                userid int4
                                            );""",
                             'staging_songs': """CREATE TABLE public.staging_songs (
                                                                num_songs int4,
                                                                artist_id varchar(256),
                                                                artist_name varchar(256),
                                                                artist_latitude numeric(18,0),
                                                                artist_longitude numeric(18,0),
                                                                artist_location varchar(256),
                                                                song_id varchar(256),
                                                                title varchar(256),
                                                                duration numeric(18,0),
                                                                "year" int4
                                                            );""",
                              'songplays':"""CREATE TABLE public.songplays (
                                                    playid varchar(32) NOT NULL,
                                                    start_time timestamp NOT NULL,
                                                    userid int4 NOT NULL,
                                                    "level" varchar(256),
                                                    songid varchar(256),
                                                    artistid varchar(256),
                                                    sessionid int4,
                                                    location varchar(256),
                                                    user_agent varchar(256),
                                                    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                                                );""",
                            'artists':"""CREATE TABLE public.artists (
                                                        artistid varchar(256) NOT NULL,
                                                        name varchar(256),
                                                        location varchar(256),
                                                        lattitude numeric(18,0),
                                                        longitude numeric(18,0)
                                                    );""",
                            'songs':"""CREATE TABLE public.songs (
                                                    songid varchar(256) NOT NULL,
                                                    title varchar(256),
                                                    artistid varchar(256),
                                                    "year" int4,
                                                    duration numeric(18,0),
                                                    CONSTRAINT songs_pkey PRIMARY KEY (songid)
                                                );
                                                """,
                            'time':"""CREATE TABLE public."time" (
                                        start_time timestamp NOT NULL,
                                        "hour" int4,
                                        "day" int4,
                                        week int4,
                                        "month" varchar(256),
                                        "year" int4,
                                        weekday varchar(256),
                                        CONSTRAINT time_pkey PRIMARY KEY (start_time)
                                    );""",
                            'users_table':""" CREATE TABLE public.users_table (
                                                userid int4 NOT NULL,
                                                first_name varchar(256),
                                                last_name varchar(256),
                                                gender varchar(256),
                                                "level" varchar(256),
                                                CONSTRAINT users_pkey PRIMARY KEY (userid)
                                            );"""}

        for table in tables:
            logging.info(f"creating {table}...")
            redshift.run(create_statements[table])
        
        logging.info(f"Done with task_id: {self.task_id}")







