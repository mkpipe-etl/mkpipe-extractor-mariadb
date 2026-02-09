from mkpipe.spark import JdbcExtractor


class MariadbExtractor(JdbcExtractor, variant='mariadb'):
    driver_name = 'mariadb'
    driver_jdbc = 'org.mariadb.jdbc.Driver'

    def build_jdbc_url(self):
        return (
            f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}'
            f'?user={self.username}&password={self.password}'
            f'&sessionVariables=sql_mode=ANSI_QUOTES'
        )
