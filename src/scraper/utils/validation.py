# src/scraper/utils/validation.py
# This file can contain more complex validation functions if Pydantic models are not enough.
# For now, Pydantic models in types.py handle much of the validation.

from typing import Any, Dict
from pydantic import ValidationError
import logging

from ..types import InsiderTrade # Use the Pydantic model for validation

logger = logging.getLogger(__name__)

def validate_trade_data(data: Dict[str, Any]) -> InsiderTrade:
    """
    Validates raw trade data against the InsiderTrade Pydantic model.
    Raises DataValidationError if validation fails.
    Returns a validated InsiderTrade object.
    """
    try:
        trade = InsiderTrade(**data)
        return trade
    except ValidationError as e:
        logger.error(f"Data validation failed for trade: {data}. Errors: {e.errors()}")
        # Depending on desired strictness, you might re-raise or return None/handle differently
        raise # Re-raise Pydantic's ValidationError or wrap it in a custom one
        # from ..exceptions import DataValidationError
        # raise DataValidationError(f"Trade data validation failed: {e}") from e