from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"


class PeriodFilters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_: Annotated[datetime, Field(alias="from")]
    to: datetime

    @field_validator("from_", "to", mode="before")
    @classmethod
    def parse_datetime(cls, value: object) -> datetime:
        if not isinstance(value, str):
            raise ValueError("Datetime must be an ISO 8601 string")
        try:
            boundary = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError("Invalid datetime format") from None
        if boundary.utcoffset() is None:
            raise ValueError("Timezone is required")
        return boundary.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.from_ >= self.to:
            raise ValueError("'from' must be less than 'to'")
        if self.to - self.from_ > timedelta(days=31):
            raise ValueError("Maximum range is 31 days")
        return self


class OrderSummaryFilters(PeriodFilters):
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None
    refresh: bool = False


class CampaignSummaryFilters(PeriodFilters):
    limit: Annotated[int | None, Field(ge=1, le=100)] = None
    refresh: bool = False


class ProductSummaryFilters(PeriodFilters):
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None
    limit: Annotated[int | None, Field(ge=1, le=100)] = None
    refresh: bool = False


class OrderTimeseriesFilters(PeriodFilters):
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None


class PaymentTimeseriesFilters(PeriodFilters):
    interval: Literal["total", "hour"] = "hour"
