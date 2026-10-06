import pytest
from pydantic import ValidationError

from data_service_processor import runtime
from data_service_processor.settings import HttpSettings, ProcessorSettings

ENVIRONMENT = {
    "PROCESSING_API_URL": "https://worker.test/api",
    "PROCESSING_API_TOKEN": "worker-token",
    "R2_ENDPOINT_URL": "https://account.r2.cloudflarestorage.com",
    "R2_ACCESS_KEY_ID": "access-key",
    "R2_SECRET_ACCESS_KEY": "secret-key",
}


@pytest.fixture
def environment(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("PROCESSOR_API_TOKEN", raising=False)


def test_dotenv_and_environment_precedence(environment, monkeypatch, tmp_path):
    monkeypatch.delenv("PROCESSING_API_URL")
    (tmp_path / ".env").write_text(
        "PROCESSING_API_URL=https://file.test/api\n"
        "PROCESSING_API_TOKEN=file-token\n"
        "UNRELATED_VALUE=ignored\n"
    )
    settings = ProcessorSettings()
    assert str(settings.processing_api_url) == "https://file.test/api"
    assert settings.processing_api_token.get_secret_value() == "worker-token"
    assert "worker-token" not in repr(settings)
    assert "secret-key" not in settings.model_dump_json()


@pytest.mark.parametrize("name", ENVIRONMENT)
def test_missing_required_setting_rejected_before_creating_clients(
    environment, monkeypatch, name
):
    monkeypatch.delenv(name)

    def unexpected_client(*args, **kwargs):
        pytest.fail("Clients must not be created with invalid configuration")

    monkeypatch.setattr(runtime.boto3, "client", unexpected_client)
    with pytest.raises(ValidationError) as error, runtime.processor_runtime():
        pytest.fail("Invalid settings must not yield a processor")
    assert error.value.errors()[0]["loc"] == (name.lower(),)


@pytest.mark.parametrize(
    "name,value",
    [
        ("PROCESSING_API_URL", "not-a-url"),
        ("R2_ENDPOINT_URL", "ftp://example.test"),
        ("PROCESSING_API_TOKEN", ""),
        ("R2_ACCESS_KEY_ID", ""),
        ("R2_SECRET_ACCESS_KEY", ""),
    ],
)
def test_invalid_environment_values_rejected(environment, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        ProcessorSettings()


def test_http_token_required_only_for_http(environment, monkeypatch):
    ProcessorSettings()
    with pytest.raises(ValidationError):
        HttpSettings()
    monkeypatch.setenv("PROCESSOR_API_TOKEN", "")
    with pytest.raises(ValidationError):
        HttpSettings()
    monkeypatch.setenv("PROCESSOR_API_TOKEN", "http-token")
    assert HttpSettings().processor_api_token.get_secret_value() == "http-token"
