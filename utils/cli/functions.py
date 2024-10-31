from rich.console import Console
from rich.prompt import FloatPrompt, IntPrompt, Prompt


def update_config(
    config: dict | None = None,
    keys: list | None = None,
    console: Console | None = None,
    updated_config: dict | None = None,
) -> dict:
    """Function that interactively and recursively updates a configuration,
    based on user input.
    Args:
        config (dict): Configuration object to update.
        keys (list): List of keys to update.
        console (Console): Rich console object for printing.
        updated_config (dict): Updated configuration object.
    Returns:
        dict: Updated configuration object.
    """

    # Initialize updated_config if this is the top-level call
    if updated_config is None:
        updated_config = {}

    for key in keys:
        to_modify = Prompt.ask(
            f":key: Would you like to modify [bold green]{key}[/bold green]? (y/n)"
        )

        if to_modify.lower() == "n":
            # Keep original value
            updated_config[key] = config.get(key)
        else:
            val_to_modify = config.get(key)
            console.print("[bold] :pencil: Modifying:\n")
            console.print(val_to_modify)

            if isinstance(val_to_modify, dict):
                # Handle nested dictionary
                updated_config[key] = {}
                update_config(
                    val_to_modify, val_to_modify.keys(), console, updated_config[key]
                )
            else:
                prompt = get_prompt_from_type(val_to_modify)
                new_val = prompt.ask(f":key: Enter new value for {key}")
                updated_config[key] = new_val
                console.print(f":light_bulb: Updated {key} to: {new_val}")

    return updated_config


def get_prompt_from_type(val: str = "") -> Prompt:
    """Function to return the correct type of Prompt
    to preserve the data type of the input.
    Args:
        val (str): Value to determine the type of Prompt to return.
    Returns:
        Prompt: Rich Prompt object.
    """
    if isinstance(val, int):
        return IntPrompt
    elif isinstance(val, float):
        return FloatPrompt
    return Prompt
