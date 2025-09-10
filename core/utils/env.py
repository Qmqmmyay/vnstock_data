import os
import sys
import json
import importlib.metadata
from ..const import PACKAGE_MAPPING
from vnii import lc_init
from vnstock_data.core.utils.const import PROJECT_DIR, ID_DIR

def get_packages_info(package_mapping=PACKAGE_MAPPING):
    "Get installed packages and their versions to customize experience."
    installed_packages = {}
    for category, packages in package_mapping.items():
        installed_packages[category] = []
        for pkg in packages:
            try:
                version = importlib.metadata.version(pkg)
                installed_packages[category].append(pkg + ' ' + version)
            except importlib.metadata.PackageNotFoundError:
                pass
    return installed_packages


class SystemInfo:
    """
    Gathers information about the interface and system.
    """
    def __init__(self):
        pass

    def _is_jpylab(self):
        try:
            shell = get_ipython().__class__.__name__
            if shell == 'ZMQInteractiveShell':
                if 'JPY_PARENT_PID' in os.environ or 'JPY_USER' in os.environ:
                    return True
            return False
        except NameError:
            return False

    def interface(self):
        """
        Determines the current interface (e.g., Terminal, Jupyter, Other).

        Returns:
            str: A string representing the current interface.
        """
        try:
            from IPython import get_ipython
            if 'IPKernelApp' not in get_ipython().config:  # Check if not in IPython kernel
                if sys.stdout.isatty():
                    return "Terminal"
                else:
                    return "Other"  # Non-interactive interface (e.g., script executed from an IDE)
            else:
                return "Jupyter"
        except (ImportError, AttributeError):
            # Fallback if IPython isn't installed or other checks fail
            if sys.stdout.isatty():
                return "Terminal"
            else:
                return "Other"

    def hosting(self):
        """
        Determines the hosting service if running in a cloud or special environment.

        Returns:
            str: A string representing the hosting service (e.g., Google Colab, Github Codespace, etc.).
        """
        try:
            if 'google.colab' in sys.modules:
                return "Google Colab"
            if self._is_jpylab():
                return "JupyterLab"
            elif 'CODESPACE_NAME' in os.environ:
                return "Github Codespace"
            elif 'GITPOD_WORKSPACE_CLUSTER_HOST' in os.environ:
                return "Gitpod"
            elif 'REPLIT_USER' in os.environ:
                return "Replit"
            elif 'KAGGLE_CONTAINER_NAME' in os.environ:
                return "Kaggle"
            elif 'SPACE_HOST' in os.environ and '.hf.space' in os.environ['SPACE_HOST']:
                return "Hugging Face Spaces"
            else:
                return "Local or Unknown"
        except KeyError:
            return "Local or Unknown"

    def os(self):
        """
        Determines the operating system.

        Returns:
            str: A string representing the operating system (e.g., Windows, Linux, macOS).
        """
        try:
            platform = sys.platform
            if platform.startswith('linux'):
                return "Linux"
            elif platform == 'darwin':
                return "macOS"
            elif platform == 'win32':
                return "Windows"
            else:
                return "Unknown"
        except Exception as e:
            return f"Error determining OS: {str(e)}"


lc_init()

def idv():
    id = PROJECT_DIR / "user.json"
    # check if the file exists
    if not os.path.exists(id):
        raise SystemExit('Không tìm thấy thông tin người dùng hợp lệ. Vui lòng liên hệ Vnstock để được hỗ trợ!')
    else:
        with open(id, 'r') as f:
            id = json.load(f)
        if not id['user']:
            raise SystemExit('Không tìm thấy thông tin người dùng hợp lệ. Vui lòng liên hệ Vnstock để được hỗ trợ!')
        return 'Valid user!'