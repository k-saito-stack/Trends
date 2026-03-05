"""Set admin custom claim on a Firebase Auth user.

This grants the user write access to /config/*, /queries/*, /change_logs/*
in Firestore (see firestore.rules isAdmin()).

Usage:
    python scripts/set_admin_claim.py --email user@kodansha.co.jp

Requires FIREBASE_SA_JSON or GOOGLE_APPLICATION_CREDENTIALS env var.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin.auth as fb_auth  # noqa: E402

from packages.core.firestore_client import _initialize  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def set_admin(email: str) -> None:
    """Set admin custom claim for the given email."""
    _initialize()

    # Look up user by email
    try:
        user = fb_auth.get_user_by_email(email)
    except fb_auth.UserNotFoundError:
        logger.error("User not found: %s", email)
        sys.exit(1)

    # Set custom claim
    fb_auth.set_custom_user_claims(user.uid, {"admin": True})
    logger.info("Admin claim set for %s (uid=%s)", email, user.uid)
    logger.info("The user must sign out and back in for the claim to take effect.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set admin custom claim")
    parser.add_argument("--email", required=True, help="User email (e.g. user@kodansha.co.jp)")
    args = parser.parse_args()

    if not args.email.endswith("@kodansha.co.jp"):
        logger.warning("Email domain is not @kodansha.co.jp — proceeding anyway.")

    set_admin(args.email)


if __name__ == "__main__":
    main()
