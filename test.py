class Food:
    def __init__(self, name, weight, calories):
        self.name = name
        self.weight = weight
        self.calories = calories


class BreadFood(Food):
    def __init__(self, name, weight, calories, white=False):
        super().__init__(name, weight, calories)
        self.weight = weight

class SoupFood(Food):
    def __init__(self, name, weight, calories, dietary=False):
        super().__init__(name, weight, calories)
        self.dietary = dietary
class FishFood(Food):
    def __init__(self,name, weight, calories,  fish):
        super().__init__(name, weight, calories)
        self.fish = fish

        