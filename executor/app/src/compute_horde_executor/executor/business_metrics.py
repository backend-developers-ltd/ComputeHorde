from django_business_metrics.v0 import BusinessMetricsManager, active_users, users

metrics_manager = BusinessMetricsManager()

metrics_manager.add(users).add(active_users)
