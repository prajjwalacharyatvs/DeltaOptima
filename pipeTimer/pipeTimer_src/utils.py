import logging
import sys
import json
import os

def setup_logger(name='pipeTimer', level_str='INFO'):
    logger = logging.getLogger(name)
    numeric_level = getattr(logging, level_str.upper(), None)

    if not isinstance(numeric_level, int):
        print(f"Warning: Invalid log level '{level_str}'. Defaulting to INFO.")
        numeric_level = logging.INFO
    
    logger.setLevel(numeric_level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

DEFAULT_CONFIG_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'config',
    'settings.json'
)

def load_config(config_path=DEFAULT_CONFIG_FILE_PATH):
    default_fallback_config = {
        "log_level": "INFO",
        "default_output_path": "output/",
        "message": "Using default fallback configuration."
    }
    try:
        abs_config_path = os.path.abspath(config_path)
        with open(abs_config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Warning: Configuration file not found at '{os.path.abspath(config_path)}'. Using default.")
        return default_fallback_config
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from '{os.path.abspath(config_path)}'. Error: {e}. Using default.")
        return default_fallback_config
    except Exception as e:
        print(f"Unexpected error loading config from '{os.path.abspath(config_path)}'. Error: {e}. Using default.")
        return default_fallback_config

if __name__ == '__main__':
    test_logger_config = load_config()
    test_logger_level = test_logger_config.get("log_level", "INFO")
    
    print(f"--- Testing Logger (Level: {test_logger_level}) ---")
    utils_logger = setup_logger(name='pipeTimerUtilsTest', level_str=test_logger_level)
    utils_logger.debug("Util Test: Debug message.")
    utils_logger.info("Util Test: Info message.")
    utils_logger.warning("Util Test: Warning message.")

    print("\n--- Testing Configuration Loader ---")
    print(f"Attempting to load default config from: {DEFAULT_CONFIG_FILE_PATH}")
    loaded_config = load_config()
    utils_logger.info(f"Default config loaded: {loaded_config}")
    
    non_existent_path_test = os.path.join(os.path.dirname(DEFAULT_CONFIG_FILE_PATH), "non_existent_settings.json")
    print(f"\nAttempting to load non-existent config from: {non_existent_path_test}")
    non_existent_config_loaded = load_config(config_path=non_existent_path_test)
    utils_logger.info(f"Non-existent config loaded: {non_existent_config_loaded}")

    malformed_path_test = os.path.join(os.path.dirname(DEFAULT_CONFIG_FILE_PATH), "malformed_settings.json")
    if os.path.exists(malformed_path_test):
        print(f"\nAttempting to load malformed config from: {malformed_path_test}")
        malformed_config_loaded = load_config(config_path=malformed_path_test)
        utils_logger.info(f"Malformed config loaded: {malformed_config_loaded}")
    else:
        utils_logger.info(f"\nSkipping malformed config test: '{malformed_path_test}' does not exist.")

    utils_logger.info("\n--- Utility tests complete ---")