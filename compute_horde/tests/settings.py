INSTALLED_APPS = ["compute_horde.receipts"]

USE_TZ = True

RECEIPT_TRANSFER_CHECKPOINT_CACHE = "default"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}