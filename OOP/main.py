# Define a Class

class Item:
    # This is the constructor method.
    # When an object of this class is created, Python calls this method automatically.
    # Therefore, any code defined inside the constructor method will be executed when an object is created.
    # We should always define the attributes of the class inside the constructor method. So that when an object is created all the attributes can be passed to the object.
    # If we are not sure what the value of an attribute will be, we can set a default value for it. e.g. quantity=0
    def __init__(self, name,price,quantity=0):
        print(f"An instance created: {name}")
        self.name = name
        self.price = price
        self.quantity = quantity

    # As we have defined the attributes in the constructor method, we can access them in any method of the class.
    def calculate_total_price(self):
        return self.price * self.quantity


# Create an Object of the Class or Instantiate the Class
item1 = Item("Phone", 100, 5)
print(item1.calculate_total_price())

item2 = Item("Laptop", 1000)
print(item2.calculate_total_price())

# Even after defining attributes inside the constructor method we can still add more attributes to the object.
item2.has_numpad = False
print(item2.has_numpad)




