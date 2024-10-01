from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):

    kafka_broker_address: str
    kafka_topic: str
    product_id: str


    # This is the first time I use this construct to load the environment variables from
    # an .env file.
    # I use another method in the past, which Jason Singer found during the last session.
        # Jason Singer
        # For those curious, an alternative way to  set the env_file in the config class would be to set the attribute via 'model_config' instead of using an inner class.
        # '''
        # model_config = {'env_file': '.env'}
        # '''
    class Config:
        env_file = ".env"

config = AppConfig()