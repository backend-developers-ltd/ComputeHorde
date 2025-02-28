variable "DEPLOY_SSH_KEY" {
  // private ssh key for cloning github repo
  type      = string
  sensitive = true
}

// variables for .env file
variable "DOTENV_SECRET_KEY" {
  type      = string
  sensitive = true
}

variable "DOTENV_POSTGRES_HOST" {
  type      = string
  sensitive = true
}

variable "DOTENV_POSTGRES_USER" {
  type      = string
  sensitive = true
}

variable "DOTENV_POSTGRES_PASSWORD" {
  type      = string
  sensitive = true
}

variable "DOTENV_DATABASE_POOL_URL" {
  type      = string
  sensitive = true
}

variable "DOTENV_DATABASE_URL" {
  type      = string
  sensitive = true
}

variable "DOTENV_SENTRY_DSN" {
  type      = string
  sensitive = true
}
