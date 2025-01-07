from django.db import models


class Validator(models.Model):
    public_key = models.TextField(unique=True)
    active = models.BooleanField()
    debug = models.BooleanField(default=False)

    def __str__(self):
        return f"hotkey: {self.public_key}"
