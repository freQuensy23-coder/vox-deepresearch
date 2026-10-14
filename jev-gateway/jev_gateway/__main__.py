import uvicorn

from .app import config_section


def main():
    uvicorn.run("jev_gateway.app:create_app", factory=True, host="127.0.0.1", port=int(config_section()["port"]), workers=1, access_log=False)


if __name__ == "__main__":
    main()
