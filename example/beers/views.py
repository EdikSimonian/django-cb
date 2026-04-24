import hashlib
import hmac
import json
import os
import time

import jwt
import requests
from django.contrib.auth.models import User
from django.db.models import Avg, Count
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.models import AccessToken, Application, RefreshToken
from oauth2_provider.settings import oauth2_settings
from oauthlib.common import generate_token
from rest_framework import permissions, serializers as drf_serializers, status, viewsets
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import AppleCredential, Beer, Brewery, Rating
from .serializers import BeerSerializer, BrewerySerializer, RatingSerializer, RegisterSerializer


# --- Permissions ---

class IsAdminGroupMember(permissions.BasePermission):
    """Allow write access only to users in the 'admin' group."""

    def has_permission(self, request, view):
        if request.method in permissions.SAFE_METHODS:
            return True
        return (
            request.user
            and request.user.is_authenticated
            and (request.user.is_superuser or request.user.groups.filter(name="admin").exists())
        )


# --- DRF API ---

class BreweryViewSet(viewsets.ModelViewSet):
    queryset = Brewery.objects.all()
    serializer_class = BrewerySerializer
    permission_classes = [IsAdminGroupMember]


class BeerViewSet(viewsets.ModelViewSet):
    queryset = Beer.objects.select_related("brewery").all()
    serializer_class = BeerSerializer
    permission_classes = [IsAdminGroupMember]

    def get_queryset(self):
        qs = super().get_queryset()
        style = self.request.query_params.get("style")
        if style:
            qs = qs.filter(style=style)
        search = self.request.query_params.get("search")
        if search:
            qs = qs.filter(name__icontains=search)
        ordering = self.request.query_params.get("ordering", "name")
        if ordering in ("name", "-name", "abv", "-abv", "avg_rating", "-avg_rating"):
            qs = qs.order_by(ordering)
        return qs


class RatingViewSet(viewsets.ModelViewSet):
    queryset = Rating.objects.all()
    serializer_class = RatingSerializer
    http_method_names = ["get", "post", "head", "options"]

    def get_permissions(self):
        if self.action == "create":
            return [permissions.IsAuthenticated()]
        return [permissions.AllowAny()]

    def get_queryset(self):
        qs = super().get_queryset()
        beer_id = self.request.query_params.get("beer_id")
        if beer_id:
            qs = qs.filter(beer_id=beer_id)
        return qs

    def perform_create(self, serializer):
        rating = serializer.save(user=self.request.user)
        # Recompute avg_rating on the beer
        stats = Rating.objects.filter(beer=rating.beer).aggregate(
            avg=Avg("score"), count=Count("id")
        )
        beer = rating.beer
        beer.avg_rating = round(stats["avg"] or 0, 1)
        beer.rating_count = stats["count"] or 0
        beer.save(update_fields=["avg_rating", "rating_count"])


class RegisterView(APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        serializer = RegisterSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        return Response(
            {"id": user.pk, "username": user.username},
            status=status.HTTP_201_CREATED,
        )


@method_decorator(csrf_exempt, name="dispatch")
class DeleteAccountView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def delete(self, request):
        user = request.user
        username = user.username
        # Revoke Apple session so the user disappears from iOS Settings →
        # Apple ID → Sign in with Apple (required by App Store 5.1.1(v)).
        # Best-effort: if it fails, we still delete the local account.
        apple_cred = AppleCredential.objects.filter(user=user).first()
        if apple_cred:
            try:
                _apple_revoke(apple_cred.refresh_token)
                print(f"[Apple] Revoke OK for {username}")
            except Exception as exc:
                print(f"[Apple] Revoke failed for {username}: {exc}")
        elif username.startswith("apple_"):
            print(
                f"[Apple] No stored refresh_token for {username} — cannot revoke "
                "Apple session. Check APPLE_TEAM_ID/APPLE_KEY_ID/APPLE_PRIVATE_KEY "
                "env vars and that iOS sent authorization_code at sign-in."
            )
        # Delete user's ratings and recompute affected beers
        from django.db import connection
        cursor = connection.cursor()
        cursor.execute(
            'DELETE FROM `beer-sample`.`_default`.`beers_rating` '
            'WHERE username = %s AND doc_type = "rating"',
            [username],
        )
        # Revoke all OAuth tokens
        AccessToken.objects.filter(user=user).delete()
        RefreshToken.objects.filter(user=user).delete()
        # Delete the user (cascades AppleCredential)
        user.delete()
        return Response({"detail": "Account deleted"}, status=status.HTTP_200_OK)


# --- Template views ---

_cache = {"styles": None, "counts": {}, "ts": 0}

def _refresh_style_cache():
    """Cache top styles and counts using ORM aggregation."""
    from django.db.models import Count
    style_counts = (
        Beer.objects.exclude(style="")
        .values("style")
        .annotate(cnt=Count("id"))
        .order_by("-cnt")
    )
    counts = {"": sum(sc["cnt"] for sc in style_counts)}
    for sc in style_counts:
        counts[sc["style"]] = sc["cnt"]
    _cache["styles"] = [sc["style"] for sc in style_counts[:10]]
    _cache["counts"] = counts
    _cache["ts"] = time.time()


def beer_list_view(request):
    from django.core.paginator import Paginator

    # Cache styles and counts for 5 minutes
    if _cache["styles"] is None or time.time() - _cache["ts"] > 300:
        _refresh_style_cache()

    style = request.GET.get("style", "")
    search = request.GET.get("q", "")

    # Pure ORM queries
    qs = Beer.objects.order_by("name")
    if style:
        qs = qs.filter(style=style)
    if search:
        qs = qs.filter(name__icontains=search)

    # Use cached count to avoid COUNT(*) on every page load
    if not search:
        total_count = _cache["counts"].get(style, _cache["counts"].get("", 0))
    else:
        total_count = qs.count()

    per_page = 48
    page_num = max(1, int(request.GET.get("page", 1)))
    num_pages = max(1, (total_count + per_page - 1) // per_page)
    page_num = min(page_num, num_pages)
    offset = (page_num - 1) * per_page

    # Sliced queryset — avoids Paginator's extra COUNT query
    beers = list(qs[offset:offset + per_page])

    # Batch-fetch brewery names for this page only
    brewery_ids = {b.brewery_id for b in beers if b.brewery_id}
    brewery_names = {}
    if brewery_ids:
        brewery_names = dict(
            Brewery.objects.filter(pk__in=brewery_ids).values_list("pk", "name")
        )
    for b in beers:
        b.brewery_display = brewery_names.get(b.brewery_id, "")

    return render(request, "beers/beer_list.html", {
        "beers": beers,
        "styles": _cache["styles"],
        "active_style": style,
        "search_query": search,
        "page_num": page_num,
        "num_pages": num_pages,
        "has_previous": page_num > 1,
        "has_next": page_num < num_pages,
        "previous_page": page_num - 1,
        "next_page": page_num + 1,
        "total_count": total_count,
        "page_range": range(1, num_pages + 1),
    })


def beer_detail_view(request, pk):
    beer = Beer.objects.select_related("brewery").get(pk=pk)
    # Query ratings directly — mobile-created ratings may lack ORM FK fields
    from django.db import connection
    cursor = connection.cursor()
    cursor.execute(
        'SELECT username, score, created_at FROM `beer-sample`.`_default`.`beers_rating` '
        'WHERE beer_id = %s AND doc_type = "rating" ORDER BY created_at DESC',
        [pk],
    )
    ratings = [
        {"username": row[0], "score": row[1], "created_at": row[2]}
        for row in cursor.fetchall()
    ]
    return render(request, "beers/beer_detail.html", {"beer": beer, "ratings": ratings})


# --- Social Login Token Exchange ---

def _get_or_create_social_user(provider, social_id, email, full_name):
    """Find or create a Django user from a social login."""
    # Try to find existing user by email first
    user = None
    if email:
        user = User.objects.filter(email=email).first()
    if not user:
        # Create username from social ID
        username = f"{provider}_{social_id[:20]}"
        if User.objects.filter(username=username).exists():
            user = User.objects.get(username=username)
        else:
            user = User.objects.create_user(
                username=username,
                email=email or "",
                password=None,  # No password — social-only account
            )
    # Populate first/last name the first time we learn it. Apple only sends
    # full_name on the very first sign-in, so we must not skip the update
    # just because the user record already existed from a prior attempt.
    # Note: save(update_fields=...) is a silent no-op on the Couchbase
    # backend — must save the full instance.
    if full_name and not (user.first_name or user.last_name):
        parts = full_name.split(" ", 1)
        user.first_name = parts[0].strip()
        if len(parts) > 1:
            user.last_name = parts[1].strip()
        user.save()
    return user


def _display_name_for(user):
    """Human-friendly display name for the id_token `name` claim."""
    full = f"{user.first_name} {user.last_name}".strip()
    if full:
        return full
    # Fall back to email local part, unless it's an Apple private-relay alias
    # (those look like `72jfv6626t@privaterelay.appleid.com` — opaque).
    if user.email and "@privaterelay.appleid.com" not in user.email:
        local = user.email.split("@", 1)[0]
        if local:
            return local
    return user.username


def _issue_oidc_tokens(user):
    """Issue OAuth2/OIDC tokens for the given user, matching DOT's format exactly."""
    import base64
    import datetime
    import uuid

    from cryptography.hazmat.primitives import serialization
    from django.conf import settings as django_settings
    from jwt.algorithms import RSAAlgorithm

    app = Application.objects.filter(client_id="brewsync-ios").first()
    if not app:
        raise ValueError("OAuth application 'brewsync-ios' not found")

    now = int(time.time())
    expires = now + oauth2_settings.ACCESS_TOKEN_EXPIRE_SECONDS

    access = AccessToken.objects.create(
        user=user,
        application=app,
        token=generate_token(),
        expires=datetime.datetime.fromtimestamp(expires),
        scope="openid profile email",
    )
    refresh = RefreshToken.objects.create(
        user=user,
        application=app,
        token=generate_token(),
        access_token=access,
    )

    # Compute kid (RFC 7638 JWK Thumbprint) — matches what DOT publishes in JWKS
    private_key = oauth2_settings.OIDC_RSA_PRIVATE_KEY
    private_key_obj = serialization.load_pem_private_key(private_key.encode(), password=None)
    public_key_obj = private_key_obj.public_key()
    jwk_dict = json.loads(RSAAlgorithm.to_jwk(public_key_obj))
    thumbprint_input = json.dumps(
        {"e": jwk_dict["e"], "kty": jwk_dict["kty"], "n": jwk_dict["n"]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    kid = base64.urlsafe_b64encode(hashlib.sha256(thumbprint_input).digest()).rstrip(b"=").decode()

    # Compute at_hash: left-half of SHA-256 of the access token, base64url-encoded
    at_digest = hashlib.sha256(access.token.encode()).digest()
    at_hash = base64.urlsafe_b64encode(at_digest[:16]).decode().rstrip("=")

    # Issuer must match what DOT serves at /.well-known/openid-configuration
    # DOT derives it from the request URL, but we don't have a request here.
    # Use DJANGO_CSRF_TRUSTED_ORIGINS or DJANGO_ALLOWED_HOSTS for the real domain.
    base_url = os.environ.get(
        "DJANGO_CSRF_TRUSTED_ORIGINS", ""
    ).split(",")[0].strip()
    if not base_url:
        host = os.environ.get("DJANGO_ALLOWED_HOSTS", "localhost").split(",")[0].strip()
        base_url = f"https://{host}"
    issuer = base_url.rstrip("/") + "/o"

    # Build claims matching DOT's exact format
    groups = list(user.groups.values_list("name", flat=True))
    claims = {
        "aud": app.client_id,            # plain string, not array
        "iat": now,
        "at_hash": at_hash,
        "sub": str(user.pk),             # DOT uses user PK as string, not username
        "iss": issuer,
        "exp": expires,
        "auth_time": now,
        "jti": str(uuid.uuid4()),
        # Custom claims (from get_additional_claims)
        "preferred_username": user.username,
        "name": _display_name_for(user),
        "email": user.email,
        "groups": groups,
    }
    id_token_jwt = jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": kid})

    return {
        "access_token": access.token,
        "id_token": id_token_jwt,
        "refresh_token": refresh.token,
        "token_type": "Bearer",
        "expires_in": oauth2_settings.ACCESS_TOKEN_EXPIRE_SECONDS,
        "scope": "openid profile email",
    }


@method_decorator(csrf_exempt, name="dispatch")
class SocialTokenExchangeView(APIView):
    """Exchange a native Apple/Google social token for Django OIDC tokens.

    POST /api/auth/social/
    {
        "provider": "apple" | "google",
        "id_token": "<JWT from Apple/Google>",
        "authorization_code": "<optional, Apple first-time>",
        "full_name": "<optional, Apple first-time>"
    }
    """
    permission_classes = [permissions.AllowAny]
    authentication_classes = []  # No auth needed — the social token IS the auth

    def post(self, request):
        provider = request.data.get("provider")
        id_token = request.data.get("id_token")

        if not provider or not id_token:
            return Response(
                {"error": "provider and id_token are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            if provider == "apple":
                social_id, email = self._verify_apple(id_token)
                full_name = request.data.get("full_name", "")
            elif provider == "google":
                social_id, email = self._verify_google(id_token)
                full_name = request.data.get("full_name", "")
            else:
                return Response(
                    {"error": "provider must be 'apple' or 'google'"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            user = _get_or_create_social_user(provider, social_id, email, full_name)

            # For Apple: if we received an authorization_code, exchange it for
            # Apple's refresh token and store it. We need this at account-
            # deletion time to revoke the user's Apple session (5.1.1(v)).
            # Best-effort — a missing refresh_token here just means revoke
            # won't work for this user. Don't fail the login.
            if provider == "apple":
                auth_code = request.data.get("authorization_code", "")
                if not auth_code:
                    print(f"[Apple] No authorization_code in sign-in payload for {user.username}")
                else:
                    try:
                        apple_refresh = _apple_exchange_code(auth_code)
                        if apple_refresh:
                            AppleCredential.objects.update_or_create(
                                user=user,
                                defaults={
                                    "apple_sub": social_id,
                                    "refresh_token": apple_refresh,
                                },
                            )
                            print(f"[Apple] Stored refresh_token for {user.username}")
                        else:
                            print(f"[Apple] Code exchange returned no refresh_token for {user.username}")
                    except Exception as exc:
                        print(f"[Apple] Code exchange failed for {user.username}: {exc}")

            tokens = _issue_oidc_tokens(user)
            return Response(tokens)

        except ValueError as exc:
            print(f"[Social] Invalid credentials ({provider}): {exc}")
            return Response(
                {"error": "Invalid credentials"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        except Exception as exc:
            import traceback
            print(f"[Social] Auth failed ({provider}): {exc}\n{traceback.format_exc()}")
            return Response(
                {"error": "Authentication failed"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

    def _verify_apple(self, id_token_str):
        """Verify Apple ID token using Apple's public keys."""
        # Fetch Apple's public keys
        apple_keys_url = "https://appleid.apple.com/auth/keys"
        resp = requests.get(apple_keys_url, timeout=10)
        resp.raise_for_status()
        apple_keys = resp.json()

        # Decode header to find the key ID
        header = jwt.get_unverified_header(id_token_str)
        kid = header.get("kid")

        # Find matching key
        key_data = None
        for key in apple_keys["keys"]:
            if key["kid"] == kid:
                key_data = key
                break
        if not key_data:
            raise ValueError("Apple public key not found")

        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(key_data)
        # Accept both App ID (native iOS) and Services ID (web) as valid audiences
        valid_audiences = [
            os.environ.get("APPLE_CLIENT_ID", "com.brewsync.auth"),
            os.environ.get("APPLE_APP_ID", "com.brewsync.app"),
        ]
        claims = jwt.decode(
            id_token_str,
            public_key,
            algorithms=["RS256"],
            audience=valid_audiences,
            issuer="https://appleid.apple.com",
        )

        return claims["sub"], claims.get("email", "")

    def _verify_google(self, id_token_str):
        """Verify Google ID token using Google's tokeninfo endpoint."""
        resp = requests.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": id_token_str},
            timeout=10,
        )
        if resp.status_code != 200:
            raise ValueError("Invalid Google ID token")

        claims = resp.json()
        expected_client_id = os.environ.get("GOOGLE_IOS_CLIENT_ID", "")
        if expected_client_id and claims.get("aud") != expected_client_id:
            raise ValueError("Google token audience mismatch")

        return claims["sub"], claims.get("email", "")


# --- Apple token revocation helpers (App Store 5.1.1(v) compliance) ---

def _apple_client_secret():
    """Build the short-lived JWT Apple requires as client_secret for
    /auth/token and /auth/revoke. Signed with the .p8 ES256 key from
    App Store Connect. Returns None if env is not configured."""
    team_id = os.environ.get("APPLE_TEAM_ID")
    key_id = os.environ.get("APPLE_KEY_ID")
    private_key = os.environ.get("APPLE_PRIVATE_KEY", "").replace("\\n", "\n")
    # For native iOS Sign In with Apple, client_id is the app bundle ID.
    client_id = os.environ.get("APPLE_APP_ID", "com.brewsync.app")

    if not (team_id and key_id and private_key):
        return None

    now = int(time.time())
    claims = {
        "iss": team_id,
        "iat": now,
        "exp": now + 15777000,  # ~6 months, Apple's max
        "aud": "https://appleid.apple.com",
        "sub": client_id,
    }
    return jwt.encode(claims, private_key, algorithm="ES256", headers={"kid": key_id})


def _apple_exchange_code(authorization_code):
    """Exchange an Apple authorizationCode for an Apple refresh_token.
    We store the refresh_token so we can later call /auth/revoke when the
    user deletes their account. Returns None if Apple keys aren't configured."""
    client_secret = _apple_client_secret()
    if not client_secret:
        return None

    client_id = os.environ.get("APPLE_APP_ID", "com.brewsync.app")
    resp = requests.post(
        "https://appleid.apple.com/auth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": authorization_code,
            "grant_type": "authorization_code",
        },
        timeout=10,
    )
    if resp.status_code != 200:
        raise ValueError(f"Apple token exchange failed: {resp.status_code} {resp.text}")
    return resp.json().get("refresh_token")


def _apple_revoke(refresh_token):
    """Revoke an Apple refresh token so the user disappears from iOS
    Settings → Apple ID → Sign in with Apple."""
    client_secret = _apple_client_secret()
    if not client_secret:
        raise ValueError("Apple keys not configured")

    client_id = os.environ.get("APPLE_APP_ID", "com.brewsync.app")
    resp = requests.post(
        "https://appleid.apple.com/auth/revoke",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "token": refresh_token,
            "token_type_hint": "refresh_token",
        },
        timeout=10,
    )
    if resp.status_code != 200:
        raise ValueError(f"Apple revoke failed: {resp.status_code} {resp.text}")
