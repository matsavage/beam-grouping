import pydantic


class ExceptionElement(pydantic.BaseModel):
    element: str
    exception_type: str
    exception_message: str

    def __init__(self, element, exception: Exception):
        super().__init__(
            element=str(element),
            exception_type=str(exception.__class__.__name__),
            exception_message=str(exception),
        )
