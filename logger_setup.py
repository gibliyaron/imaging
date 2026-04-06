import logging


def setup_logger(config):
    """Configures and returns the centralized logger."""
    logging.basicConfig(
        level=config['logging']['level'],
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config['logging']['file']),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("ETL_Worker")
