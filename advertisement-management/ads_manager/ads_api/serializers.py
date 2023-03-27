from rest_framework import serializers
from .models import advertisement

class ads_Serializer(serializers.ModelSerializer):
    class Meta:
        model = advertisement
        fields = ('id', 'description', 'email', 'state', 'category')