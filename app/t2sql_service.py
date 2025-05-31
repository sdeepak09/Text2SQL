from pathlib import Path
from nemoguardrails import LLMRails, RailsConfig

# Ensure the guardrails directory is correctly referenced relative to this file
guardrails_path = Path(__file__).parent / "guardrails"
cfg = RailsConfig(config_paths=[guardrails_path]) # config_paths expects a list

# Dummy _raw_t2sql function as it's marked as "existing"
def _raw_t2sql(question: str) -> str:
    # This is a placeholder for the actual Text-to-SQL logic
    # print(f"[_raw_t2sql] Processing question: {question}")
    if question is None: # Guard against None if `revised` can be None and `ok` is True
        return "Error: No valid question provided to _raw_t2sql."
    return f"SELECT simulated_data FROM table WHERE condition = '{question}';"

rails = LLMRails(config=cfg) # Initialize with the config object

# --- Monkey-patching `run_input` to `rails` instance for compatibility with tests ---
# This is a workaround based on the discrepancy between the issue description and LLMRails API.
def custom_run_input(self, text: str):
    # `check_input` is a real method that returns True if allowed, False otherwise.
    is_allowed = self.check_input(text)
    if is_allowed:
        # If allowed, `run_input` is expected to return the (possibly revised) input.
        # `check_input` doesn't revise. We'll return original text for now.
        return True, text
    else:
        # If not allowed, `run_input` returns False. The second element is often a message or None.
        # The tests use `ok, _`, so the content of the second element when `ok` is False might not be critical
        # for the test's pass/fail condition regarding `ok`, but it's good practice to return None or an error message.
        # The `safeguarded_t2sql` function provides the specific error message.
        return False, None

# Bind this function as a method to the `rails` instance.
import types
rails.run_input = types.MethodType(custom_run_input, rails)
# --- End of monkey-patch ---

def safeguarded_t2sql(question: str) -> str:
    # This function uses the (now monkey-patched) rails.run_input method.
    ok, revised_question = rails.run_input(question)

    if not ok:
        # This is the specific refusal message expected by the logic in the issue.
        return "❌ Sorry, that request isn’t allowed."

    # If `revised_question` is None when `ok` is True (e.g. if `custom_run_input` was changed to return that)
    # `_raw_t2sql` needs to handle it. Current `custom_run_input` returns original `text`.
    return _raw_t2sql(revised_question)
