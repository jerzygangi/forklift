# Decorator that ensures a named argument "options" has all of the
# required_options
def ensure_required_options_exist(required_options):
	# This is just a wrapper decorator around confirm_options_dictionary_has_required_options(...)
	def decorator_wrapper_for_confirm_options_dictionary_has_required_options(func):
		# Provided there is an array called required_options somehow in scope,
		# this function will ensure all strings in required_options are present
		# in a dictionary called "options" inside kwargs
		def confirm_options_dictionary_has_required_options(*args, **kwargs):
			# Step 1: Ensure we have the "options" dictionary
			if "options" not in kwargs.keys():
				raise RequiredKeyOptionsWasntProvidedException
			elif not isinstance(kwargs["options"], dict):
				raise OptionsIsntADictionaryException
			# Step 2: Check that all options exist, and bomb if they don't
			# (Return silently if they do)
			else:
				options = kwargs["options"]
				keys_in_options = options.keys()
				are_all_options_present = not False in [required_option in keys_in_options for required_option in required_options]
				if not are_all_options_present:
					raise RequiredOptionsArentAllPresentException
				else:
					return func(*args, **kwargs)
					
		return confirm_options_dictionary_has_required_options
	return decorator_wrapper_for_confirm_options_dictionary_has_required_options

# Custom exception class for ...
class RequiredOptionsArentAllPresentException(Exception):
	pass

# Custom exception class for ...
class RequiredKeyOptionsWasntProvidedException(Exception):
	pass

# Custom exception class for ...
class OptionsIsntADictionaryException(Exception):
	pass
