from mkpipe.functions_spark import BaseExtractor


class MariadbExtractor(BaseExtractor):
    def __init__(self, config, settings):
        super().__init__(
            config,
            settings,
            driver_name='mariadb',
            driver_jdbc='org.mariadb.jdbc.Driver',
        )

    def build_jdbc_url(self):
        return f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.password}&sessionVariables=sql_mode=ANSI_QUOTES'
