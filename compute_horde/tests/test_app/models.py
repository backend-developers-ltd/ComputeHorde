from django.db import models


class Validator(models.Model):
    public_key = models.TextField(unique=True)
    active = models.BooleanField()
    debug = models.BooleanField(default=False)

    def __str__(self):
        return f"hotkey: {self.public_key}"


# stats-collector uses this definition of the validator model
class Validator2(models.Model):
    ss58_address = models.CharField(max_length=48, unique=True)
    is_active = models.BooleanField()

    def __str__(self):
        return f"hotkey: {self.ss58_address}"
