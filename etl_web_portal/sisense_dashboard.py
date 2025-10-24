# sisense_dashboard.py
import jwt
import time
import uuid
import secrets
import hashlib
from flask import render_template_string

class SisenseDashboard:
    def __init__(self):
        self.config = {
            'shared_secret': "4d0f10040c9030b5ce5407db54a42343700f3457d46c6e8e9fed370e82fee7fa",
            'user_id': "atyab@orbis.com",
            'dashboard_id': "684fcf585beae193fd1da6dc",
            'tenant_id': "683560a181b8cd75aea4c030",
            'tenant_name': "orbis",
            'sisense_base_url': "https://cannyalley.sisense.com"
        }
    
    def generate_secure_jti(self):
        """Generate a unique, secure JTI (JWT ID)"""
        raw = f"{uuid.uuid4()}-{time.time()}-{secrets.token_hex(16)}"
        return hashlib.sha256(raw.encode()).hexdigest()
    
    def generate_jwt_payload(self, user_role="viewer"):
        current_time = int(time.time())
        return {
            "iat": current_time,
            "exp": current_time + 1800,  # 30 minutes
            "sub": self.config['user_id'],
            "email": self.config['user_id'],
            "firstName": "Atyab",
            "username": self.config['user_id'],
            "groups": [user_role],
            "jti": self.generate_secure_jti(),
            "tenantId": self.config['tenant_id'],
            "dashboard": self.config['dashboard_id'],
            "embed": True
        }
    
    def generate_jwt_token(self, user_role="viewer"):
        """Generate JWT token for Sisense SSO"""
        try:
            payload = self.generate_jwt_payload(user_role)
            token = jwt.encode(payload, self.config['shared_secret'], algorithm="HS256")
            
            # Handle bytes token if needed
            if isinstance(token, bytes):
                token = token.decode('utf-8')
            return token
        except Exception as e:
            print(f"JWT generation error: {e}")
            return None
    
    def construct_embed_urls(self, token):
        """Construct all types of embed URLs"""
        if not token:
            return None
            
        return_to = "%2Forbis%2Fapp%2Fmain%2Fdashboards%2F684fcf585beae193fd1da6dc%3Fembed%3Dtrue%26l%3Dtrue%26theme%3D687f389bc3b8934f2a79e79a"

        url_with_tenant_name = (
            f"{self.config['sisense_base_url']}/{self.config['tenant_name']}/jwt?"
            f"jwt={token}&return_to={return_to}"
        )
        
        return {
            "with_tenant_name": url_with_tenant_name,
            "direct_iframe": f"{self.config['sisense_base_url']}/orbis/app/main/dashboards/684fcf585beae193fd1da6dc?embed=true&theme=687f389bc3b8934f2a79e79a",
            "sdk_embed": f"{self.config['sisense_base_url']}/orbis/app/main/dashboards/684fcf585beae193fd1da6dc"
        }
    
    def get_dashboard_html(self, embed_type="iframe"):
        """Get HTML for different embedding types"""
        token = self.generate_jwt_token()
        if not token:
            return "<p>Error generating dashboard access</p>"
        
        urls = self.construct_embed_urls(token)
        
        if embed_type == "iframe":
            return f'''
            <div style="width: 100%; height: 800px;">
                <iframe src="{urls['direct_iframe']}" width="100%" height="100%" frameborder="0"></iframe>
            </div>
            '''
        elif embed_type == "sdk":
            return f'''
            <div id="dashboardContainer"></div>
            <script src="{self.config['sisense_base_url']}/js/embed/sisense.js"></script>
            <script>
                SisenseSDK.on('ready', function() {{
                    SisenseSDK.Embed('dashboardContainer').dashboard('{self.config['dashboard_id']}').render();
                }});
            </script>
            '''
        else:
            return f'<a href="{urls["with_tenant_name"]}" target="_blank">Open Dashboard</a>'
    
    def create_dashboard_page(self, title="Dashboard"):
        """Create a complete dashboard page"""
        token = self.generate_jwt_token()
        urls = self.construct_embed_urls(token) if token else None
        
        html_template = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>{{ title }}</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
                .header { background: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
                .btn { display: inline-block; padding: 10px 15px; margin: 5px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; }
                .btn-success { background: #28a745; }
                iframe { width: 100%; height: 800px; border: none; border-radius: 10px; background: white; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{{ title }}</h1>
                <div>
                    <a href="/upload" class="btn">← Back to Upload</a>
                    {% if urls %}
                    <a href="{{ urls.direct_iframe }}" class="btn btn-success" target="_blank">Open in New Tab</a>
                    {% endif %}
                </div>
            </div>
            
            {% if urls %}
            <iframe src="{{ urls.direct_iframe }}" title="Sisense Dashboard"></iframe>
            {% else %}
            <p>Error loading dashboard</p>
            {% endif %}
        </body>
        </html>
        '''
        
        return render_template_string(html_template, title=title, urls=urls)

# Create global instance
dashboard_manager = SisenseDashboard()