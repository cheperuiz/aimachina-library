# pylint: disable=import-error
import io
import base64
from PIL import Image

# pylint: enable=import-error

DATA_URL_PREFIX = "data:image/jpeg;base64,"


def image_to_bytes(img):
    stream = io.BytesIO()
    img.save(stream, "JPEG")
    return stream.getvalue()


def image_to_base64(img, prefix=""):
    bytes_ = image_to_bytes(img) if type(img) is not bytes else img
    return prefix + str(base64.b64encode(bytes_), "utf-8")


def base64_to_image(bytes_):
    bytes_ = bytes_.split(",")[-1]  # Remove the dataurl prefix
    decoded = base64.b64decode(bytes_)
    return bytes_to_image(decoded)


def bytes_to_image(bytes_):
    img = Image.open(io.BytesIO(bytes_))
    img.verify()
    img = Image.open(io.BytesIO(bytes_)).convert("RGB")
    return img


def decode_image(src):
    if type(src) is bytes:
        return bytes_to_image(src)
    return base64_to_image(src)
