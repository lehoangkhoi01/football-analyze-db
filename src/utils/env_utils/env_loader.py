def get_env(env_file=".env.local"):
    """
    Load environment variables from a .env file.
    """
    from dotenv import load_dotenv
    import os

    # Load environment variables from .env file
    load_dotenv(env_file)

    # Optional: Print all loaded environment variables for debugging purposes
    # for key, value in os.environ.items():
    #     print(f"{key}={value}")
    
    return os.environ