import secrets
import time

import jwt
from allauth.account.views import SignupView as AllauthSignupView
from bittensor import Keypair
from constance import config
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from django.db.models import QuerySet
from django.forms.models import ModelForm
from django.http import HttpRequest
from django.http.response import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.utils.decorators import method_decorator
from django.views.generic import CreateView, DetailView, ListView
from fingerprint.views import fingerprint
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .forms import DockerImageJobForm, GenerateAPITokenForm, RawScriptJobForm
from .models import Job, Miner, Validator
from .authentication import is_hotkey_allowed

JWT_LIFETIME = 3600

NONCE_LIFETIME = 300


class SignupView(AllauthSignupView):
    """Regular view but redirects if public registration is disabled"""

    def dispatch(self, *args, **kwargs) -> HttpResponse:
        if not config.ENABLE_PUBLIC_REGISTRATION:
            return render(self.request, "allauth/signup_disabled.html")
        return super().dispatch(*args, **kwargs)


@method_decorator(login_required, name="dispatch")
class JobListView(ListView):
    model = Job
    template_name = "core/job_list.html"
    context_object_name = "jobs"
    paginate_by = 20

    def get_queryset(self) -> QuerySet:
        return self.request.user.jobs.with_statuses().order_by("-created_at")


@method_decorator(fingerprint, name="get")
class JobCreateView(CreateView):
    model = Job

    @method_decorator(login_required)
    def dispatch(self, request: HttpRequest, *args, **kwargs) -> HttpResponse:
        if not config.ENABLE_ORGANIC_JOBS:
            return render(request, "core/job_create_disabled.html")
        return super().dispatch(request, *args, **kwargs)

    def form_valid(self, form: ModelForm) -> HttpResponse:
        try:
            job = form.save(commit=False)
            job.user = self.request.user
            job.save()
        except Validator.DoesNotExist:
            messages.add_message(
                self.request,
                messages.ERROR,
                "No validators available, please try again later",
            )
            return super().get(self.request)
        except Miner.DoesNotExist:
            messages.add_message(
                self.request,
                messages.ERROR,
                "No miners available, please try again later",
            )
            return super().get(self.request)

        return HttpResponseRedirect(job.get_absolute_url())


class DockerImageJobCreateView(JobCreateView):
    form_class = DockerImageJobForm
    template_name = "core/job_create_docker_image.html"

    def get_form_kwargs(self) -> dict:
        """Prefill the form with the values from the referenced job"""

        form_kwargs = super().get_form_kwargs()
        if reference_pk := self.request.GET.get("ref"):
            job = get_object_or_404(Job, pk=reference_pk)
            form_kwargs.setdefault("initial", {}).update(
                {
                    "docker_image": job.docker_image,
                    "raw_script": job.raw_script,
                    "args": job.args,
                    "env": job.env,
                    "use_gpu": job.use_gpu,
                    "input_url": job.input_url,
                    "hf_repo_id": job.hf_repo_id,
                    "hf_revision": job.hf_revision,
                }
            )
        return form_kwargs


class RawScriptJobCreateView(JobCreateView):
    form_class = RawScriptJobForm
    template_name = "core/job_create_raw_script.html"

    def get_form_kwargs(self) -> dict:
        """Prefill the form with the values from the referenced job"""

        form_kwargs = super().get_form_kwargs()
        if reference_pk := self.request.GET.get("ref"):
            job = get_object_or_404(Job, pk=reference_pk)
            form_kwargs.setdefault("initial", {}).update(
                {
                    "raw_script": job.raw_script,
                    "input_url": job.input_url,
                    "hf_repo_id": job.hf_repo_id,
                    "hf_revision": job.hf_revision,
                }
            )
        return form_kwargs


@method_decorator(fingerprint, name="get")
class JobDetailView(DetailView):
    model = Job
    template_name = "core/job_detail.html"
    context_object_name = "job"

    def get_queryset(self) -> QuerySet:
        return super().get_queryset().with_statuses()

    def get_object(self, queryset: QuerySet | None = None) -> Job:
        """
        Regenerate download URL if it's expired before displaying the object to user
        """
        job: Job = super().get_object(queryset)
        if job.is_download_url_expired():
            job.save()
        return job


@login_required
def api_token_view(request):
    user = request.user

    always_show_token = False
    is_new_token = False
    if request.method == "POST":
        token, created = Token.objects.get_or_create(user=user)
        if not created:
            token.delete()
            token = Token.objects.create(user=user)
        is_new_token = True
    else:
        token = Token.objects.filter(user=user).first()

    token_key = None
    if token is not None:
        token_key = token.key if is_new_token or always_show_token else "*************"
    context = {
        "form": GenerateAPITokenForm(),
        "token": token,
        "safe_token_key": token_key,
        "is_new_token": is_new_token,
        "always_show_token": always_show_token,
    }
    return render(request, "core/api_token.html", context)


@api_view(["GET"])
def auth_nonce_view(request):
    signer = TimestampSigner()
    nonce = secrets.token_hex(16)
    signed_nonce = signer.sign(nonce)
    return Response({"nonce": signed_nonce})


@api_view(["POST"])
def auth_login_view(request):
    hotkey = request.data.get("hotkey")
    signature = request.data.get("signature")
    signed_nonce = request.data.get("nonce")
    if not hotkey:
        return Response({"error": "Missing hotkey"}, status=status.HTTP_400_BAD_REQUEST)
    if not signature:
        return Response({"error": "Missing signature"}, status=status.HTTP_400_BAD_REQUEST)
    if not signed_nonce:
        return Response({"error": "Missing nonce"}, status=status.HTTP_400_BAD_REQUEST)

    signer = TimestampSigner()
    nonce_lifetime = settings.NONCE_LIFETIME
    try:
        signer.unsign(signed_nonce, max_age=nonce_lifetime)
    except BadSignature:
        return Response({"error": "Invalid nonce"}, status=status.HTTP_400_BAD_REQUEST)
    except SignatureExpired:
        return Response({"error": "Nonce expired"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        is_valid = Keypair(ss58_address=hotkey).verify(data=signed_nonce.encode(), signature=bytes.fromhex(signature))
    except Exception:
        return Response({"error": "Nonce expired or not found"}, status=status.HTTP_400_BAD_REQUEST)

    if not is_valid:
        return Response({"error": "Invalid signature"}, status=status.HTTP_401_UNAUTHORIZED)

    jwt_lifetime = settings.JWT_LIFETIME
    payload = {
        "sub": hotkey,
        "iat": int(time.time()),
        "exp": int(time.time()) + jwt_lifetime,
    }
    token = jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")

    return Response({"token": token})
