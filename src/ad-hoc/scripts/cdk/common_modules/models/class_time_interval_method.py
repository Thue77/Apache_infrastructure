import datetime
import pydantic
import inspect


class ClassTimeIntervalMethod(pydantic.BaseModel):
    '''
    Interface for object that holds the information needed to execute a class method

    Args:
        from_dataset_name (str): Name of the source dataset
        to_dataset_name (str): Name of the dataset related to the delta state is located
        group_name (str): Name of the folder in which the dataset related to the delta state is located
        to_layer (str): Name of the layer in which the dataset related to the delta state is located
        spark (SparkSession): SparkSession - Must be configured with the correct storage account access
        delta_entity_name (str): Name of the entity
        delta_path (str): Path to the delta table. Varies depending on selection of storage solution
    '''
    class_instance: object
    method_name: str
    method_kwargs: dict

    
    # Check if method exists
    @pydantic.model_validator(mode='after')
    def check_method_exists(self) -> str:
        getattr(self.class_instance, self.method_name)
        return self
    
    # Check if method has start and end arguments
    @pydantic.model_validator(mode='after')
    def check_datetime_method_kwargs_exist(self) -> dict:
        method_args = list(inspect.signature(getattr(self.class_instance, self.method_name)).parameters)
        test_method_datetime_args = ['start', 'end'] == method_args[0:2]

        if not test_method_datetime_args:
            raise ValueError(f"TypeError {self.method_name} does not have arguments start and end")
        return self
    
    # Check if any arguments in method_kwargs do not exist in method arguments
    @pydantic.model_validator(mode='after')
    def check_redundant_method_kwargs(self) -> dict:
        method_args = list(inspect.signature(getattr(self.class_instance, self.method_name)).parameters)
        for key in self.method_kwargs.keys():
            if key not in method_args:
                raise TypeError(f"Method {self.method_name} does not have argument {key}")
        return self
    
    # Check if any default method arguments except "start" and "end" are not included in method_kwargs
    @pydantic.model_validator(mode='after')
    def check_missing_method_kwargs(self) -> dict:
        default_args = [k for k,v in inspect.signature(getattr(self.class_instance, self.method_name)).parameters.items() if v.default is inspect.Parameter.empty]
        default_args = default_args[2:] # Remove "start" and "end" from list
        for key in default_args:
            if key not in self.method_kwargs.keys():
                raise TypeError(f"Method kwargs does not have required argument {key}")
        return self
    
    # Check that start and end are datetime objects
    @pydantic.model_validator(mode='after')
    def check_datetime_method_kwargs(self) -> dict:
        default_args = [(k,v.annotation) for k,v in inspect.signature(getattr(self.class_instance, self.method_name)).parameters.items() if v.default is inspect.Parameter.empty]
        default_args = default_args[0:2] # Remove "start" and "end" from list
        for name,annotation in default_args:
            if annotation!=datetime.datetime:
                raise TypeError(f"Argument {name} is not of type datetime.datetime")
        return self

    

if __name__=='__main__':
    # test = Test()
    # test_method = ClassTimeIntervalMethod(class_instance=test, method_name='test', method_kwargs={'start':datetime.datetime(2023,9,10), 'end':'test', 'b':'test'})
    # print([(k,isinstance(k,str),v.annotation==datetime.datetime) for k,v in inspect.signature(getattr(test, 'test')).parameters.items() if v.default is inspect.Parameter.empty])
    pass

