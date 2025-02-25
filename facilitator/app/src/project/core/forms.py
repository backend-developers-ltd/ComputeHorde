from crispy_forms.helper import FormHelper
from crispy_forms.layout import Submit
from django import forms

from .models import Job


class DockerImageJobForm(forms.ModelForm):
    class Meta:
        model = Job
        fields = (
            "docker_image",
            "args",
            "env",
            "use_gpu",
            "input_url",
            "hf_repo_id",
            "hf_revision",
        )
        widgets = {
            "args": forms.Textarea(attrs={"rows": 2}),
            "env": forms.Textarea(attrs={"rows": 2}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_method = "post"
        self.helper.add_input(Submit("submit", "Submit"))


class RawScriptJobForm(forms.ModelForm):
    class Meta:
        model = Job
        fields = (
            "raw_script",
            "input_url",
            "hf_repo_id",
            "hf_revision",
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_method = "post"
        self.helper.add_input(Submit("submit", "Submit"))


class GenerateAPITokenForm(forms.Form):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_method = "post"
        self.helper.add_input(Submit("generate", "Generate Token"))
