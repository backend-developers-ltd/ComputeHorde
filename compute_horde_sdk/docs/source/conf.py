# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "ComputeHorde SDK"
copyright = "2025, Backend Developers Ltd"
author = "Backend Developers Ltd"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx_multiversion",
]

autodoc_typehints = "description"
autodoc_class_signature = "separated"

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "exclude-members": "model_config,__new__,__init__",  # Pydantic models
}

templates_path = ["_templates"]
exclude_patterns = ["Thumbs.db", ".DS_Store"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "prev_next_buttons_location": "both",
    "collapse_navigation": True,
}

# -- Multiversion configuration ----------------------------------------------
# https://sphinx-contrib.github.io/multiversion/main/configuration.html

smv_tag_whitelist = r"^(draft/)?sdk\-v.*$"
smv_branch_whitelist = r"^(master|main)$"
smv_remote_whitelist = r"^.*$"
smv_released_pattern = r"^refs/tags/sdk\-v.*$"
