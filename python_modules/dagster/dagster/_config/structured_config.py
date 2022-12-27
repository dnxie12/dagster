import inspect

from dagster._config.source import BoolSource, IntSource, StringSource
from dagster._core.definitions.definition_config_schema import IDefinitionConfigSchema

try:
    from functools import cached_property
except ImportError:

    class cached_property:  # type: ignore[no-redef]
        pass


from abc import ABC, abstractmethod
from typing import Any, Optional, Type, cast

from pydantic import BaseModel
from pydantic.fields import SHAPE_SINGLETON, ModelField

import dagster._check as check
from dagster import Field, Shape
from dagster._config.field_utils import (
    FIELD_NO_DEFAULT_PROVIDED,
    config_dictionary_from_values,
    convert_potential_field,
)
from dagster._core.definitions.resource_definition import ResourceDefinition, ResourceFunction
from dagster._core.storage.io_manager import IOManager, IOManagerDefinition


class MakeConfigCacheable(
    BaseModel,
    # Various pydantic model config (https://docs.pydantic.dev/usage/model_config/)
    # Necessary to allow for caching decorators
    arbitrary_types_allowed=True,
    # Avoid pydantic reading a cached property class as part of the schema
    keep_untouched=(cached_property,),
    # Ensure the class is serializable, for caching purposes
    frozen=True,
):
    """This class centralizes and implements all the chicanery we need in order
    to support caching decorators. If we decide this is a bad idea we can remove it
    all in one go.
    """

    def __setattr__(self, name: str, value: Any):
        # This is a hack to allow us to set attributes on the class that are not part of the
        # config schema. Pydantic will normally raise an error if you try to set an attribute
        # that is not part of the schema.

        if name.startswith("_") or name.endswith("_cache"):
            object.__setattr__(self, name, value)
            return

        return super().__setattr__(name, value)


class Config(MakeConfigCacheable):
    """
    Base class for Dagster configuration models.
    """


def _curry_config_schema(schema_field: Field, data: Any) -> IDefinitionConfigSchema:
    """Return a new config schema configured with the passed in data"""

    # We don't do anything with this resource definition, other than
    # use it to construct configured schema
    inner_resource_def = ResourceDefinition(lambda _: None, schema_field)
    configured_resource_def = inner_resource_def.configured(
        config_dictionary_from_values(
            data,
            schema_field,
        ),
    )
    # this cast required to make mypy happy, with does not support Self
    configured_resource_def = cast(ResourceDefinition, configured_resource_def)
    return configured_resource_def.config_schema


class Resource(
    ResourceDefinition,
    Config,
):
    """
    Base class for Dagster resources that utilize structured config.

    This class is a subclass of both :py:class:`ResourceDefinition` and :py:class:`Config`, and
    provides a default implementation of the resource_fn that returns the resource itself.

    Example:

    .. code-block:: python

        class WriterResource(Resource):
            prefix: str

            def output(self, text: str) -> None:
                print(f"{self.prefix}{text}")

    """

    def __init__(self, **data: Any):
        schema = infer_schema_from_config_class(self.__class__)
        Config.__init__(self, **data)
        ResourceDefinition.__init__(
            self,
            resource_fn=self.create_object_to_pass_to_user_code,
            config_schema=_curry_config_schema(schema, data),
            description=self.__doc__,
        )

    def create_object_to_pass_to_user_code(self, context) -> Any:  # pylint: disable=unused-argument
        # This acts identically to the function decorator with @resource in the old style
        #
        # Default behavior, for "new-style" resources, is to return itself, passing
        # the actual resource object to user code.
        return self


class StructuredResourceAdapter(Resource, ABC):
    """
    Adapter base class for wrapping a decorated, function-style resource
    with structured config.

    To use this class, subclass it, define config schema fields using Pydantic,
    and implement the ``wrapped_resource`` method.

    Example:

    .. code-block:: python

        @resource(config_schema={"prefix": str})
        def writer_resource(context):
            prefix = context.resource_config["prefix"]

            def output(text: str) -> None:
                out_txt.append(f"{prefix}{text}")

            return output

        class WriterResource(StructuredResourceAdapter):
            prefix: str

            @property
            def wrapped_resource(self) -> ResourceDefinition:
                return writer_resource
    """

    @property
    @abstractmethod
    def wrapped_resource(self) -> ResourceDefinition:
        raise NotImplementedError()

    @property
    def resource_fn(self) -> ResourceFunction:
        return self.wrapped_resource.resource_fn

    def __call__(self, *args, **kwargs):
        return self.wrapped_resource(*args, **kwargs)


class StructuredConfigIOManagerBase(IOManagerDefinition, Config, ABC):
    """
    Base class for Dagster IO managers that utilize structured config. This base class
    is useful for cases in which the returned IO manager is not the same as the class itself
    (e.g. when it is a wrapper around the actual IO manager implementation).

    This class is a subclass of both :py:class:`IOManagerDefinition` and :py:class:`Config`.
    Implementers should provide an implementation of the :py:meth:`resource_function` method,
    which should return an instance of :py:class:`IOManager`.
    """

    def __init__(self, **data: Any):
        schema = infer_schema_from_config_class(self.__class__)
        Config.__init__(self, **data)
        IOManagerDefinition.__init__(
            self,
            resource_fn=self.create_io_manager_to_pass_to_user_code,
            config_schema=_curry_config_schema(schema, data),
            description=self.__doc__,
        )

    @abstractmethod
    def create_io_manager_to_pass_to_user_code(self, context) -> IOManager:
        """Implement as one would implement a @io_manager decorator function"""
        raise NotImplementedError()


class StructuredConfigIOManager(StructuredConfigIOManagerBase, IOManager):
    """
    Base class for Dagster IO managers that utilize structured config.

    This class is a subclass of both :py:class:`IOManagerDefinition`, :py:class:`Config`,
    and :py:class:`IOManager`. Implementers must provide an implementation of the
    :py:meth:`handle_output` and :py:meth:`load_input` methods.
    """

    def create_io_manager_to_pass_to_user_code(self, context) -> IOManager:
        return self


def _convert_pydantic_field(pydantic_field: ModelField) -> Field:
    """
    Transforms a Pydantic field into a corresponding Dagster config field.
    """

    if _safe_is_subclass(pydantic_field.type_, Config):
        return infer_schema_from_config_class(
            pydantic_field.type_, description=pydantic_field.field_info.description
        )

    if pydantic_field.shape != SHAPE_SINGLETON:
        raise NotImplementedError(f"Pydantic shape {pydantic_field.shape} not supported")

    potential_dagster_type = pydantic_field.type_

    # special case raw python literals to their source equivalents
    if potential_dagster_type is str:
        inner_config_type = StringSource
    elif potential_dagster_type is int:
        inner_config_type = IntSource  # type: ignore
    elif potential_dagster_type is bool:
        inner_config_type = BoolSource  # type: ignore
    else:
        inner_config_type = convert_potential_field(potential_dagster_type).config_type

    return Field(
        config=inner_config_type,
        description=pydantic_field.field_info.description,
        is_required=_is_pydantic_field_required(pydantic_field),
        default_value=pydantic_field.default
        if pydantic_field.default
        else FIELD_NO_DEFAULT_PROVIDED,
    )


def _is_pydantic_field_required(pydantic_field: ModelField) -> bool:
    # required is BoolUndefined = Union[bool, UndefinedType] in Pydantic
    if isinstance(pydantic_field.required, bool):
        return pydantic_field.required

    raise Exception(
        "pydantic.field.required is their UndefinedType sentinel value which we do not fully understand the semantics right now. For the time being going to throw an error to figure see when we actually encounter this state."
    )


class StructuredIOManagerAdapter(StructuredConfigIOManagerBase):
    @property
    @abstractmethod
    def wrapped_io_manager(self) -> IOManagerDefinition:
        raise NotImplementedError()

    def create_io_manager_to_pass_to_user_code(self, context) -> IOManager:
        raise Exception("called?")

    @property
    def resource_fn(self) -> ResourceFunction:
        return self.wrapped_io_manager.resource_fn


def infer_schema_from_config_annotation(model_cls: Any, config_arg_default: Any) -> Field:
    """
    Parses a structured config class or primitive type and returns a corresponding Dagster config Field.
    """

    if _safe_is_subclass(model_cls, Config):
        check.invariant(
            config_arg_default is inspect.Parameter.empty,
            "Cannot provide a default value when using a Config class",
        )
        return infer_schema_from_config_class(model_cls)

    inner_config_type = convert_potential_field(model_cls).config_type
    return Field(
        config=inner_config_type,
        default_value=FIELD_NO_DEFAULT_PROVIDED
        if config_arg_default is inspect.Parameter.empty
        else config_arg_default,
    )


def _safe_is_subclass(cls: Any, possible_parent_cls: Type) -> bool:
    """Version of issubclass that returns False if cls is not a Type."""
    if not isinstance(cls, type):
        return False
    return issubclass(cls, possible_parent_cls)


def infer_schema_from_config_class(
    model_cls: Type[Config], description: Optional[str] = None
) -> Field:
    """
    Parses a structured config class and returns a corresponding Dagster config Field.
    """

    check.param_invariant(
        issubclass(model_cls, Config),
        "Config type annotation must inherit from dagster._config.structured_config.Config",
    )

    fields = {}
    for pydantic_field in model_cls.__fields__.values():
        fields[pydantic_field.alias] = _convert_pydantic_field(pydantic_field)

    docstring = model_cls.__doc__.strip() if model_cls.__doc__ else None
    return Field(config=Shape(fields), description=description or docstring)
