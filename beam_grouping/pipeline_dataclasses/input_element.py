from datetime import datetime

import pydantic


class InputElement(pydantic.BaseModel):
    id: int
    timestamp: datetime
    value: float
