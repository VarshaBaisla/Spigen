import jwt
import time
import uuid
import secrets
from datetime import datetime, timezone
import ntplib
import requests
import hashlib

class SisenseJWTGenerator:
    def __init__(self):
        self.shared_secret = "4d0f10040c9030b5ce5407db54a42343700f3457d46c6e8e9fed370e82fee7fa"
        self.user_id = "atyab@orbis.com"
        self.dashboard_id = "684fcf585beae193fd1da6dc"
        self.tenant_id = "683560a181b8cd75aea4c030"
        self.tenant_name = "orbis"
        self.sisense_base_url = "https://cannyalley.sisense.com"
        self.max_clock_skew = 300  # 5 minutes in seconds

    def generate_secure_jti(self):
        """Generate a unique, secure JTI (JWT ID)"""
        raw = f"{uuid.uuid4()}-{time.time()}-{secrets.token_hex(16)}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def sync_time_with_ntp(self):
        try:
            client = ntplib.NTPClient()
            response = client.request('pool.ntp.org')
            return response.tx_time
        except Exception as e:
            print(f"Warning: NTP sync failed - {str(e)}")
            return None

    def verify_time_sync(self):
        system_time = time.time()
        ntp_time = self.sync_time_with_ntp()
        if ntp_time:
            time_diff = abs(system_time - ntp_time)
            if time_diff > self.max_clock_skew:
                print(f"Warning: Significant time difference detected - System: {system_time}, NTP: {ntp_time}")
                return False
        return True

    def generate_jwt_payload(self):
        current_time = int(time.time())
        return {
            "iat": current_time,
            "exp": current_time + 1800,  # 30 minutes
            "sub": self.user_id,
            "email": self.user_id,
            
            "groups": ["viewer"],
            "jti": self.generate_secure_jti(),  # ✅ Ensures unique jti value
            "tenantId": self.tenant_id,

            "dashboard": self.dashboard_id,
            "embed":True
        }

    def generate_jwt_token(self):
        if not self.verify_time_sync():
            print("Warning: Proceeding with potentially unsynced system time")

        payload = self.generate_jwt_payload()
        token = jwt.encode(payload, self.shared_secret, algorithm="HS256")

        try:
            decoded = jwt.decode(token, self.shared_secret, algorithms=['HS256'])
            print("JWT token successfully generated and verified")
            print(f"JTI: {decoded['jti']}")
            return token
        except jwt.ExpiredSignatureError:
            print("Error: Generated token is already expired - check system clock")
            return None
        except Exception as e:
            print(f"Error validating JWT: {str(e)}")
            return None

    def construct_embed_urls(self, token):
        if not token:
            return None
        return_to ="%2Forbis%2Fapp%2Fmain%2Fdashboards%2F6863a360fa6d79a3ff80e6b5%3Fembed%3Dtrue%26l%3Dtrue%26theme%3D687f389bc3b8934f2a79e79a"

        url_with_tenant_name = (
            f"{self.sisense_base_url}/{self.tenant_name}/jwt?"
            f"jwt={token}&return_to={return_to}"
        )
        return {
            "with_tenant_name": url_with_tenant_name
        }

   

    def run(self):
        print("Starting JWT generation process...")
        token = self.generate_jwt_token()
        if not token:
            return

        print("\nGenerated JWT Token:")
        print(token)

        urls = self.construct_embed_urls(token)
        if not urls:
            return

        print("\nEmbed URLs:")
        print(f"1. With tenantName: {urls['with_tenant_name']}")


if __name__ == "__main__":
    generator = SisenseJWTGenerator()
generator.run()